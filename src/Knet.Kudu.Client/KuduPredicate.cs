using System;
using System.Collections.Generic;
using Google.Protobuf;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client;

/// <summary>
/// A predicate which can be used to filter rows based on the value of a column.
/// </summary>
public class KuduPredicate : IEquatable<KuduPredicate>
{
    /// <summary>
    /// The inclusive lower bound value if this is a Range predicate, or
    /// the createEquality value if this is an Equality predicate.
    /// </summary>
    internal byte[]? Lower { get; }

    /// <summary>
    /// The exclusive upper bound value if this is a Range predicate.
    /// </summary>
    internal byte[]? Upper { get; }

    /// <summary>
    /// In-list values.
    /// </summary>
    internal SortedSet<byte[]>? InListValues { get; }

    /// <summary>
    /// The list of bloom filters in this predicate.
    /// </summary>
    internal List<KuduBloomFilter>? BloomFilters { get; }

    public PredicateType Type { get; }

    public ColumnSchema Column { get; }

    /// <summary>
    /// Constructor for all non IN list predicates.
    /// </summary>
    /// <param name="type">The predicate type.</param>
    /// <param name="column">The column to which the predicate applies.</param>
    /// <param name="lower">
    /// The lower bound serialized value if this is a Range predicate,
    /// or the equality value if this is an Equality predicate.
    /// </param>
    /// <param name="upper">The upper bound serialized value if this is a Range predicate.</param>
    public KuduPredicate(PredicateType type, ColumnSchema column, byte[]? lower, byte[]? upper)
    {
        Type = type;
        Column = column;
        Lower = lower;
        Upper = upper;
    }

    /// <summary>
    /// Constructor for IN list predicate.
    /// </summary>
    /// <param name="column">The column to which the predicate applies.</param>
    /// <param name="inListValues">The encoded IN list values.</param>
    public KuduPredicate(ColumnSchema column, SortedSet<byte[]> inListValues)
    {
        Column = column;
        Type = PredicateType.InList;
        InListValues = inListValues;
    }

    /// <summary>
    /// Constructor for in bloom filter predicate.
    /// </summary>
    /// <param name="bloomFilters"></param>
    /// <param name="lower">
    /// The lower bound serialized value if this is a Range predicate,
    /// or the equality value if this is an Equality predicate.
    /// </param>
    /// <param name="upper">The upper bound serialized value if this is a Range predicate.</param>
    public KuduPredicate(List<KuduBloomFilter> bloomFilters, byte[]? lower, byte[]? upper)
    {
        ColumnSchema? column = null;
        foreach (var bloomFilter in bloomFilters)
        {
            if (column is not null && !column.Equals(bloomFilter.Column))
            {
                throw new ArgumentException("Bloom filters must be for the same column", nameof(bloomFilters));
            }

            column = bloomFilter.Column;
        }

        Column = column ?? throw new ArgumentException("Missing bloom filters", nameof(bloomFilters));
        Type = PredicateType.InBloomFilter;
        BloomFilters = bloomFilters;
        Lower = lower;
        Upper = upper;
    }

    public bool Equals(KuduPredicate? other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return Type == other.Type &&
            Column.Equals(other.Column) &&
            Lower.SequenceEqual(other.Lower) &&
            Upper.SequenceEqual(other.Upper) &&
            InListEquals(other.InListValues) &&
            InBloomFiltersEquals(other.BloomFilters);
    }

    private bool InListEquals(SortedSet<byte[]>? other)
    {
        if (InListValues is null && other is null)
            return true;

        if (InListValues is null || other is null)
            return false;

        return InListValues.SetEquals(other);
    }

    private bool InBloomFiltersEquals(List<KuduBloomFilter>? other)
    {
        if (BloomFilters is null && other is null)
            return true;

        if (BloomFilters is null || other is null)
            return false;

        if (BloomFilters.Count != other.Count)
            return false;

        for (int i = 0; i < other.Count; i++)
        {
            var bf1 = BloomFilters[i];
            var bf2 = other[i];

            if (!bf1.BloomFilter.Equals(bf2.BloomFilter))
                return false;
        }

        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as KuduPredicate);

    public override int GetHashCode() => HashCode.Combine(Type, Column);

    /// <summary>
    /// Merges another <see cref="KuduPredicate"/> into this one, returning a new
    /// <see cref="KuduPredicate"/> which matches the logical intersection (AND)
    /// of the input predicates.
    /// </summary>
    /// <param name="other">The predicate to merge with this predicate.</param>
    public KuduPredicate Merge(KuduPredicate other)
    {
        if (!Column.Equals(other.Column))
            throw new ArgumentException("Predicates from different columns may not be merged");

        // First, consider other.Type == None, IsNotNull, or IsNull
        // None predicates dominate.
        if (other.Type == PredicateType.None)
            return other;

        // IsNotNull is dominated by all other predicates,
        // except IsNull, for which the merge is None.
        if (other.Type == PredicateType.IsNotNull)
            return Type == PredicateType.IsNull ? None(Column) : this;

        // IsNull merged with any predicate type besides itself is None.
        if (other.Type == PredicateType.IsNull)
            return Type == PredicateType.IsNull ? this : None(Column);

        // Now other.Type == Equality, Range, InList, or InBloomFilter.
        switch (Type)
        {
            case PredicateType.None: return this;
            case PredicateType.IsNotNull: return other;
            case PredicateType.IsNull: return None(Column);
            case PredicateType.Equality:
                if (other.Type == PredicateType.Equality)
                {
                    if (Compare(Column, Lower!, other.Lower!) != 0)
                    {
                        return None(Column);
                    }
                    else
                    {
                        return this;
                    }
                }
                else if (other.Type == PredicateType.Range)
                {
                    if (other.RangeContains(Lower!))
                    {
                        return this;
                    }
                    else
                    {
                        return None(Column);
                    }
                }
                else if (other.Type == PredicateType.InList || other.Type == PredicateType.InBloomFilter)
                {
                    return other.Merge(this);
                }
                else
                {
                    throw new Exception($"Unknown predicate type {other.Type}");
                }
            case PredicateType.Range:
                if (other.Type == PredicateType.Equality ||
                    other.Type == PredicateType.InList ||
                    other.Type == PredicateType.InBloomFilter)
                {
                    return other.Merge(this);
                }
                else if (other.Type == PredicateType.Range)
                {
                    var (newLower, newUpper) = MergeRange(other);

                    if (newLower != null && newUpper != null && Compare(Column, newLower, newUpper) >= 0)
                    {
                        return None(Column);
                    }
                    else
                    {
                        if (newLower != null && newUpper != null && AreConsecutive(newLower, newUpper))
                        {
                            return new KuduPredicate(PredicateType.Equality, Column, newLower, null);
                        }
                        else
                        {
                            return new KuduPredicate(PredicateType.Range, Column, newLower, newUpper);
                        }
                    }
                }
                else
                {
                    throw new Exception($"Unknown predicate type {other.Type}");
                }
            case PredicateType.InList:
                if (other.Type == PredicateType.Equality)
                {
                    // The equality value needs to be a member of the InList
                    if (InListValues!.Contains(other.Lower!))
                    {
                        return other;
                    }
                    else
                    {
                        return None(Column);
                    }
                }
                else if (other.Type == PredicateType.Range)
                {
                    var comparer = new PredicateComparer(Column);
                    var values = new SortedSet<byte[]>(comparer);
                    foreach (var value in InListValues!)
                    {
                        if (other.RangeContains(value))
                        {
                            values.Add(value);
                        }
                    }
                    return BuildInList(Column, values);
                }
                else if (other.Type == PredicateType.InList)
                {
                    var comparer = new PredicateComparer(Column);
                    var values = new SortedSet<byte[]>(comparer);
                    foreach (var value in InListValues!)
                    {
                        if (other.InListValues!.Contains(value))
                        {
                            values.Add(value);
                        }
                    }
                    return BuildInList(Column, values);
                }
                else if (other.Type == PredicateType.InBloomFilter)
                {
                    return other.Merge(this);
                }
                else
                {
                    throw new Exception($"Unknown predicate type {other.Type}");
                }
            case PredicateType.InBloomFilter:
                if (other.Type == PredicateType.Equality)
                {
                    if (CheckValueInBloomFilter(other.Lower))
                    {
                        // Value falls in bloom filters so change to Equality predicate.
                        return other;
                    }
                    else
                    {
                        // Value does not fall in bloom filters.
                        return None(Column);
                    }
                }
                else if (other.Type == PredicateType.InList)
                {
                    var comparer = new PredicateComparer(Column);
                    var values = new SortedSet<byte[]>(comparer);
                    foreach (var value in other.InListValues!)
                    {
                        if (CheckValueInBloomFilter(value))
                        {
                            values.Add(value);
                        }
                    }
                    return BuildInList(Column, values);
                }
                else if (other.Type == PredicateType.Range || other.Type == PredicateType.InBloomFilter)
                {
                    var (newLower, newUpper) = MergeRange(other);

                    if (newLower != null && newUpper != null && Compare(Column, newLower, newUpper) >= 0)
                    {
                        return None(Column);
                    }
                    else
                    {
                        if (newLower != null && newUpper != null && AreConsecutive(newLower, newUpper) &&
                            CheckValueInBloomFilter(newLower))
                        {
                            return new KuduPredicate(PredicateType.Equality, Column, newLower, null);
                        }
                        else
                        {
                            // Other could be Range and may not have bloom filters.
                            var newBloomFilters = new List<KuduBloomFilter>(
                                BloomFilters!.Count + other.BloomFilters?.Count ?? 0);

                            newBloomFilters.AddRange(BloomFilters);

                            if (other.BloomFilters != null)
                                newBloomFilters.AddRange(other.BloomFilters);

                            return NewInBloomFilterPredicate(newBloomFilters, newLower, newUpper);
                        }
                    }
                }
                else
                {
                    throw new Exception($"Unknown predicate type {other.Type}");
                }
            default:
                throw new Exception($"Unknown predicate type {Type}");
        }
    }

    public ColumnPredicatePB ToProtobuf()
    {
        var predicate = new ColumnPredicatePB { Column = Column.Name };

        switch (Type)
        {
            case PredicateType.Equality:
                predicate.Equality = new ColumnPredicatePB.Types.Equality
                {
                    Value = UnsafeByteOperations.UnsafeWrap(Lower)
                };
                break;

            case PredicateType.Range:
                predicate.Range = new ColumnPredicatePB.Types.Range();
                if (Lower != null)
                {
                    predicate.Range.Lower = UnsafeByteOperations.UnsafeWrap(Lower);
                }
                if (Upper != null)
                {
                    predicate.Range.Upper = UnsafeByteOperations.UnsafeWrap(Upper);
                }
                break;

            case PredicateType.IsNotNull:
                predicate.IsNotNull = new ColumnPredicatePB.Types.IsNotNull();
                break;

            case PredicateType.IsNull:
                predicate.IsNull = new ColumnPredicatePB.Types.IsNull();
                break;

            case PredicateType.InList:
                predicate.InList = new ColumnPredicatePB.Types.InList();

                var values = predicate.InList.Values;
                values.Capacity = InListValues!.Count;

                foreach (var value in InListValues)
                {
                    values.Add(UnsafeByteOperations.UnsafeWrap(value));
                }

                break;

            case PredicateType.InBloomFilter:
                predicate.InBloomFilter = new ColumnPredicatePB.Types.InBloomFilter();
                if (Lower != null)
                {
                    predicate.InBloomFilter.Lower = UnsafeByteOperations.UnsafeWrap(Lower);
                }
                if (Upper != null)
                {
                    predicate.InBloomFilter.Upper = UnsafeByteOperations.UnsafeWrap(Upper);
                }

                var bloomFilters = predicate.InBloomFilter.BloomFilters;
                bloomFilters.Capacity = BloomFilters!.Count;

                foreach (var bf in BloomFilters)
                {
                    bloomFilters.Add(bf.ToProtobuf());
                }

                break;

            case PredicateType.None:
                throw new Exception("Can not convert None predicate to protobuf message");

            default:
                throw new Exception($"Unknown predicate type {Type}");
        }

        return predicate;
    }

    public override string ToString()
    {
        var name = Column.Name;

        switch (Type)
        {
            case PredicateType.Equality: return $"`{name}` = {ValueToString(Lower!)}";
            case PredicateType.Range: return RangeToString();
            case PredicateType.InList:
                {
                    var strings = new List<string>(InListValues!.Count);
                    foreach (var value in InListValues)
                        strings.Add(ValueToString(value));
                    return $"`{name}` IN ({string.Join(", ", strings)})";
                }
            case PredicateType.InBloomFilter:
                {
                    if (Lower != null || Upper != null)
                    {
                        var range = RangeToString();
                        return $"{range} AND `{name}` IN {BloomFilters!.Count} BLOOM FILTERS";
                    }
                    else
                    {
                        return $"`{name}` IN {BloomFilters!.Count} BLOOM FILTERS";
                    }
                }
            case PredicateType.IsNotNull: return $"`{name}` IS NOT NULL";
            case PredicateType.IsNull: return $"`{name}` IS NULL";
            case PredicateType.None: return $"`{name}` NONE";
            default: throw new Exception($"Unknown predicate type {Type}");
        }
    }

    private string RangeToString()
    {
        var name = Column.Name;

        if (Lower is null)
        {
            return $"`{name}` < {ValueToString(Upper!)}";
        }
        else if (Upper is null)
        {
            return $"`{name}` >= {ValueToString(Lower)}";
        }
        else
        {
            return $"`{name}` >= {ValueToString(Lower)} AND `{name}` < {ValueToString(Upper)}";
        }
    }

    /// <summary>
    /// Returns the string value of serialized value according to the type of column.
    /// </summary>
    /// <param name="value">The value.</param>
    private string ValueToString(byte[] value)
    {
        return Column.Type switch
        {
            KuduType.Bool => KuduEncoder.DecodeBool(value).ToString(),
            KuduType.Int8 => KuduEncoder.DecodeInt8(value).ToString(),
            KuduType.Int16 => KuduEncoder.DecodeInt16(value).ToString(),
            KuduType.Int32 => KuduEncoder.DecodeInt32(value).ToString(),
            KuduType.Int64 => KuduEncoder.DecodeInt64(value).ToString(),
            KuduType.UnixtimeMicros => KuduEncoder.DecodeDateTime(value).ToString(),
            KuduType.Date => KuduEncoder.DecodeDate(value).ToString(),
            KuduType.Float => KuduEncoder.DecodeFloat(value).ToString(),
            KuduType.Double => KuduEncoder.DecodeDouble(value).ToString(),
            KuduType.String => $@"""{KuduEncoder.DecodeString(value)}""",
            KuduType.Varchar => $@"""{KuduEncoder.DecodeString(value)}""",
            KuduType.Binary => BitConverter.ToString(value),
            KuduType.Decimal32 => KuduEncoder.DecodeDecimal32(value, GetScale()).ToString(),
            KuduType.Decimal64 => KuduEncoder.DecodeDecimal64(value, GetScale()).ToString(),
            KuduType.Decimal128 => KuduEncoder.DecodeDecimal128(value, GetScale()).ToString(),

            _ => throw new Exception($"Unknown column type {Column.Type}")
        };

        int GetScale() => Column.TypeAttributes!.Scale.GetValueOrDefault();
    }

    /// <summary>
    /// Creates a new <see cref="KuduPredicate"/> on a boolean column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, bool value)
    {
        KuduTypeValidation.ValidateColumnType(column, KuduType.Bool);

        // Create the comparison predicate. Range predicates on boolean values can
        // always be converted to either an equality, an IS NOT NULL (filtering only
        // null values), or NONE (filtering all values).

        return op switch
        {
            ComparisonOp.Equal => EqualPredicate(column, value),

            // b > true  -> b NONE
            // b > false -> b = true
            ComparisonOp.Greater when value => None(column),
            ComparisonOp.Greater when !value => EqualPredicate(column, true),

            // b >= true  -> b = true
            // b >= false -> b IS NOT NULL
            ComparisonOp.GreaterEqual when value => EqualPredicate(column, true),
            ComparisonOp.GreaterEqual when !value => NewIsNotNullPredicate(column),

            // b < true  -> b = false
            // b < false -> b NONE
            ComparisonOp.Less when value => EqualPredicate(column, false),
            ComparisonOp.Less when !value => None(column),

            // b <= true  -> b IS NOT NULL
            // b <= false -> b = false
            ComparisonOp.LessEqual when value => NewIsNotNullPredicate(column),
            ComparisonOp.LessEqual when !value => EqualPredicate(column, false),

            _ => throw new Exception($"Unknown ComparisonOp {op}")
        };
    }

    /// <summary>
    /// Creates a new comparison predicate on an integer or timestamp column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, long value)
    {
        KuduTypeValidation.ValidateColumnType(column,
            KuduTypeFlags.Int8 |
            KuduTypeFlags.Int16 |
            KuduTypeFlags.Int32 |
            KuduTypeFlags.Int64 |
            KuduTypeFlags.Date |
            KuduTypeFlags.UnixtimeMicros);

        long minValue = MinIntValue(column.Type);
        long maxValue = MaxIntValue(column.Type);

        if (value < minValue || value > maxValue)
        {
            throw new ArgumentException($"Integer value out of range for {column}");
        }

        return NewComparisonPredicate(column, op, value, minValue, maxValue);
    }

    /// <summary>
    /// Creates a new comparison predicate on an integer or timestamp column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    /// <param name="minValue">The minimum value for the column.</param>
    /// <param name="maxValue">The maximum value for the column.</param>
    private static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, long value, long minValue, long maxValue)
    {
        if (op == ComparisonOp.LessEqual)
        {
            if (value == maxValue)
            {
                // If the value can't be incremented because it is at the top end of the
                // range, then substitute the predicate with an IS NOT NULL predicate.
                // This has the same effect as an inclusive upper bound on the maximum
                // value. If the column is not nullable then the IS NOT NULL predicate
                // is ignored.
                return NewIsNotNullPredicate(column);
            }
            value += 1;
            op = ComparisonOp.Less;
        }
        else if (op == ComparisonOp.Greater)
        {
            if (value == maxValue)
            {
                return None(column);
            }
            value += 1;
            op = ComparisonOp.GreaterEqual;
        }

        var bytes = GetBinary(column.Type, value);

        return op switch
        {
            ComparisonOp.GreaterEqual when value == minValue => NewIsNotNullPredicate(column),
            ComparisonOp.GreaterEqual when value == maxValue => EqualPredicate(column, bytes),
            ComparisonOp.GreaterEqual => new KuduPredicate(PredicateType.Range, column, bytes, null),

            ComparisonOp.Equal => EqualPredicate(column, bytes),

            ComparisonOp.Less when value == minValue => None(column),
            ComparisonOp.Less => new KuduPredicate(PredicateType.Range, column, null, bytes),

            _ => throw new Exception($"Unknown ComparisonOp {op}")
        };
    }

    /// <summary>
    /// Creates a new comparison predicate on a timestamp column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, DateTime value)
    {
        long rawValue;
        var type = column.Type;

        if (type == KuduType.UnixtimeMicros)
        {
            rawValue = EpochTime.ToUnixTimeMicros(value);
        }
        else if (type == KuduType.Date)
        {
            rawValue = EpochTime.ToUnixTimeDays(value);
        }
        else
        {
            KuduTypeValidation.ThrowException(column,
                KuduTypeFlags.UnixtimeMicros | KuduTypeFlags.Date);

            rawValue = 0;
        }

        return NewComparisonPredicate(column, op, rawValue);
    }

    /// <summary>
    /// Creates a new comparison predicate on a float column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, float value)
    {
        KuduTypeValidation.ValidateColumnType(column, KuduType.Float);

        if (op == ComparisonOp.LessEqual)
        {
            if (value == float.PositiveInfinity)
            {
                return NewIsNotNullPredicate(column);
            }
            value = value.NextUp();
            op = ComparisonOp.Less;
        }
        else if (op == ComparisonOp.Greater)
        {
            if (value == float.PositiveInfinity)
            {
                return None(column);
            }
            value = value.NextUp();
            op = ComparisonOp.GreaterEqual;
        }

        byte[] bytes = KuduEncoder.EncodeFloat(value);
        switch (op)
        {
            case ComparisonOp.GreaterEqual:
                if (value == float.NegativeInfinity)
                {
                    return NewIsNotNullPredicate(column);
                }
                else if (value == float.PositiveInfinity)
                {
                    return new KuduPredicate(PredicateType.Equality, column, bytes, null);
                }
                return new KuduPredicate(PredicateType.Range, column, bytes, null);
            case ComparisonOp.Equal:
                return new KuduPredicate(PredicateType.Equality, column, bytes, null);
            case ComparisonOp.Less:
                if (value == float.NegativeInfinity)
                {
                    return None(column);
                }
                return new KuduPredicate(PredicateType.Range, column, null, bytes);
            default:
                throw new Exception($"Unknown ComparisonOp {op}");
        }
    }

    /// <summary>
    /// Creates a new comparison predicate on a double column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, double value)
    {
        KuduTypeValidation.ValidateColumnType(column, KuduType.Double);

        if (op == ComparisonOp.LessEqual)
        {
            if (value == double.PositiveInfinity)
            {
                return NewIsNotNullPredicate(column);
            }
            value = value.NextUp();
            op = ComparisonOp.Less;
        }
        else if (op == ComparisonOp.Greater)
        {
            if (value == double.PositiveInfinity)
            {
                return None(column);
            }
            value = value.NextUp();
            op = ComparisonOp.GreaterEqual;
        }

        byte[] bytes = KuduEncoder.EncodeDouble(value);
        switch (op)
        {
            case ComparisonOp.GreaterEqual:
                if (value == double.NegativeInfinity)
                {
                    return NewIsNotNullPredicate(column);
                }
                else if (value == double.PositiveInfinity)
                {
                    return new KuduPredicate(PredicateType.Equality, column, bytes, null);
                }
                return new KuduPredicate(PredicateType.Range, column, bytes, null);
            case ComparisonOp.Equal:
                return new KuduPredicate(PredicateType.Equality, column, bytes, null);
            case ComparisonOp.Less:
                if (value == double.NegativeInfinity)
                {
                    return None(column);
                }
                return new KuduPredicate(PredicateType.Range, column, null, bytes);
            default:
                throw new Exception($"Unknown ComparisonOp {op}");
        }
    }

    /// <summary>
    /// Creates a new comparison predicate on a decimal column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, decimal value)
    {
        KuduTypeValidation.ValidateColumnType(column,
            KuduTypeFlags.Decimal32 |
            KuduTypeFlags.Decimal64 |
            KuduTypeFlags.Decimal128);

        var typeAttributes = column.TypeAttributes!;
        int precision = typeAttributes.Precision.GetValueOrDefault();
        int scale = typeAttributes.Scale.GetValueOrDefault();

        long maxValue;
        long longValue;

        switch (column.Type)
        {
            case KuduType.Decimal32:
                maxValue = DecimalUtil.MaxDecimal32(precision);
                longValue = DecimalUtil.EncodeDecimal32(value, precision, scale);
                break;

            case KuduType.Decimal64:
                maxValue = DecimalUtil.MaxDecimal64(precision);
                longValue = DecimalUtil.EncodeDecimal64(value, precision, scale);
                break;

            case KuduType.Decimal128:
                return NewComparisonPredicate(column, op,
                    DecimalUtil.EncodeDecimal128(value, precision, scale),
                    DecimalUtil.MinDecimal128(precision),
                    DecimalUtil.MaxDecimal128(precision));

            default:
                throw new Exception($"Unknown column type {column.Type}");
        }

        long minValue = maxValue * -1;

        if (value < minValue || value > maxValue)
        {
            throw new ArgumentException($"Decimal value out of range for {column}");
        }

        return NewComparisonPredicate(column, op, longValue, minValue, maxValue);
    }

    private static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, Int128 value, Int128 minValue, Int128 maxValue)
    {
        if (op == ComparisonOp.LessEqual)
        {
            if (value == maxValue)
            {
                // If the value can't be incremented because it is at the top end of the
                // range, then substitute the predicate with an IS NOT NULL predicate.
                // This has the same effect as an inclusive upper bound on the maximum
                // value. If the column is not nullable then the IS NOT NULL predicate
                // is ignored.
                return NewIsNotNullPredicate(column);
            }
            value += 1;
            op = ComparisonOp.Less;
        }
        else if (op == ComparisonOp.Greater)
        {
            if (value == maxValue)
            {
                return None(column);
            }
            value += 1;
            op = ComparisonOp.GreaterEqual;
        }

        var bytes = KuduEncoder.EncodeInt128(value);

        return op switch
        {
            ComparisonOp.GreaterEqual when value == minValue => NewIsNotNullPredicate(column),
            ComparisonOp.GreaterEqual when value == maxValue => EqualPredicate(column, bytes),
            ComparisonOp.GreaterEqual => new KuduPredicate(PredicateType.Range, column, bytes, null),

            ComparisonOp.Equal => EqualPredicate(column, bytes),

            ComparisonOp.Less when value == minValue => None(column),
            ComparisonOp.Less => new KuduPredicate(PredicateType.Range, column, null, bytes),

            _ => throw new Exception($"Unknown ComparisonOp {op}")
        };
    }

    /// <summary>
    /// Creates a new comparison predicate on a string column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, string value)
    {
        KuduTypeValidation.ValidateColumnType(column,
            KuduTypeFlags.String | KuduTypeFlags.Varchar);

        var bytes = KuduEncoder.EncodeString(value);
        return NewComparisonPredicateNoCheck(column, op, bytes);
    }

    /// <summary>
    /// Creates a new comparison predicate on a binary column.
    /// </summary>
    /// <param name="column">The column schema.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    public static KuduPredicate NewComparisonPredicate(
        ColumnSchema column, ComparisonOp op, byte[] value)
    {
        KuduTypeValidation.ValidateColumnType(column, KuduType.Binary);

        return NewComparisonPredicateNoCheck(column, op, value);
    }

    private static KuduPredicate NewComparisonPredicateNoCheck(
        ColumnSchema column, ComparisonOp op, byte[] value)
    {
        if (op == ComparisonOp.LessEqual)
        {
            Array.Resize(ref value, value.Length + 1);
            op = ComparisonOp.Less;
        }
        else if (op == ComparisonOp.Greater)
        {
            Array.Resize(ref value, value.Length + 1);
            op = ComparisonOp.GreaterEqual;
        }

        return op switch
        {
            ComparisonOp.GreaterEqual when value.Length == 0 => NewIsNotNullPredicate(column),
            ComparisonOp.GreaterEqual => new KuduPredicate(PredicateType.Range, column, value, null),

            ComparisonOp.Equal => EqualPredicate(column, value),

            ComparisonOp.Less when value.Length == 0 => None(column),
            ComparisonOp.Less => new KuduPredicate(PredicateType.Range, column, null, value),

            _ => throw new Exception($"Unknown ComparisonOp {op}")
        };
    }

    public static KuduPredicate None(ColumnSchema column)
    {
        return new KuduPredicate(PredicateType.None, column, null, null);
    }

    private static KuduPredicate EqualPredicate(ColumnSchema column, bool value)
    {
        var binary = KuduEncoder.EncodeBool(value);
        return EqualPredicate(column, binary);
    }

    private static KuduPredicate EqualPredicate(ColumnSchema column, byte[] value)
    {
        return new KuduPredicate(PredicateType.Equality, column, value, null);
    }

    /// <summary>
    /// Creates a new IsNotNull predicate.
    /// </summary>
    /// <param name="column">The column that the predicate applies to.</param>
    public static KuduPredicate NewIsNotNullPredicate(ColumnSchema column)
    {
        return new KuduPredicate(PredicateType.IsNotNull, column, null, null);
    }

    /// <summary>
    /// Creates a new IsNull predicate.
    /// </summary>
    /// <param name="column">The column that the predicate applies to.</param>
    public static KuduPredicate NewIsNullPredicate(ColumnSchema column)
    {
        if (!column.IsNullable)
            return None(column);

        return new KuduPredicate(PredicateType.IsNull, column, null, null);
    }

    public static KuduPredicate NewInListPredicate<T>(
        ColumnSchema column, IEnumerable<T> values)
    {
        var comparer = new PredicateComparer(column);
        var encoded = new SortedSet<byte[]>(comparer);

        if (typeof(T) == typeof(string) || typeof(T) == typeof(byte[]))
        {
            switch (values)
            {
                case IEnumerable<string> vals:
                    {
                        KuduTypeValidation.ValidateColumnType(column,
                            KuduTypeFlags.String | KuduTypeFlags.Varchar);
                        foreach (var value in vals)
                        {
                            encoded.Add(KuduEncoder.EncodeString(value));
                        }
                        break;
                    }
                case IEnumerable<byte[]> vals:
                    {
                        KuduTypeValidation.ValidateColumnType(column, KuduType.Binary);
                        foreach (var value in vals)
                        {
                            encoded.Add(value);
                        }
                        break;
                    }
                default:
                    break;
            }
        }
        else if (typeof(T) == typeof(DateTime))
        {
            var type = column.Type;
            if (type == KuduType.UnixtimeMicros)
            {
                foreach (var value in values)
                {
                    encoded.Add(KuduEncoder.EncodeDateTime((DateTime)(object)value!));
                }
            }
            else if (type == KuduType.Date)
            {
                foreach (var value in values)
                {
                    encoded.Add(KuduEncoder.EncodeDate((DateTime)(object)value!));
                }
            }
            else
            {
                KuduTypeValidation.ThrowException(column,
                    KuduTypeFlags.UnixtimeMicros | KuduTypeFlags.Date);
            }
        }
        else if (typeof(T) == typeof(decimal))
        {
            var type = column.Type;
            var typeAttributes = column.TypeAttributes!;
            var precision = typeAttributes.Precision.GetValueOrDefault();
            var scale = typeAttributes.Scale.GetValueOrDefault();

            if (type == KuduType.Decimal32)
            {
                foreach (var value in values)
                {
                    encoded.Add(KuduEncoder.EncodeDecimal32(
                        (decimal)(object)value!, precision, scale));
                }
            }
            else if (type == KuduType.Decimal64)
            {
                foreach (var value in values)
                {
                    encoded.Add(KuduEncoder.EncodeDecimal64(
                        (decimal)(object)value!, precision, scale));
                }
            }
            else if (type == KuduType.Decimal128)
            {
                foreach (var value in values)
                {
                    encoded.Add(KuduEncoder.EncodeDecimal128(
                        (decimal)(object)value!, precision, scale));
                }
            }
            else
            {
                KuduTypeValidation.ThrowException(column,
                    KuduTypeFlags.Decimal32 |
                    KuduTypeFlags.Decimal64 |
                    KuduTypeFlags.Decimal128);
            }
        }
        else
        {
            foreach (var value in values)
            {
                if (typeof(T) == typeof(bool))
                {
                    KuduTypeValidation.ValidateColumnType(column, KuduType.Bool);
                    encoded.Add(KuduEncoder.EncodeBool((bool)(object)value!));
                }
                else if (typeof(T) == typeof(sbyte))
                {
                    KuduTypeValidation.ValidateColumnType(column, KuduType.Int8);
                    encoded.Add(KuduEncoder.EncodeInt8((sbyte)(object)value!));
                }
                else if (typeof(T) == typeof(byte))
                {
                    KuduTypeValidation.ValidateColumnType(column, KuduType.Int8);
                    encoded.Add(KuduEncoder.EncodeUInt8((byte)(object)value!));
                }
                else if (typeof(T) == typeof(short))
                {
                    KuduTypeValidation.ValidateColumnType(column, KuduType.Int16);
                    encoded.Add(KuduEncoder.EncodeInt16((short)(object)value!));
                }
                else if (typeof(T) == typeof(int))
                {
                    KuduTypeValidation.ValidateColumnType(column,
                        KuduTypeFlags.Int32 | KuduTypeFlags.Date);
                    encoded.Add(KuduEncoder.EncodeInt32((int)(object)value!));
                }
                else if (typeof(T) == typeof(long))
                {
                    KuduTypeValidation.ValidateColumnType(column,
                        KuduTypeFlags.Int64 | KuduTypeFlags.UnixtimeMicros);
                    encoded.Add(KuduEncoder.EncodeInt64((long)(object)value!));
                }
                else if (typeof(T) == typeof(float))
                {
                    KuduTypeValidation.ValidateColumnType(column, KuduType.Float);
                    encoded.Add(KuduEncoder.EncodeFloat((float)(object)value!));
                }
                else if (typeof(T) == typeof(double))
                {
                    KuduTypeValidation.ValidateColumnType(column, KuduType.Double);
                    encoded.Add(KuduEncoder.EncodeDouble((double)(object)value!));
                }
                else
                {
                    throw new ArgumentException(
                        $"Type {typeof(T).Name} isn't supported for IN list values");
                }
            }
        }

        return BuildInList(column, encoded);
    }

    public static KuduPredicate NewInBloomFilterPredicate(
        List<KuduBloomFilter> bloomFilters,
        byte[]? lower = null,
        byte[]? upper = null)
    {
        return new KuduPredicate(bloomFilters, lower, upper);
    }

    internal static long MinIntValue(KuduType type)
    {
        return type switch
        {
            KuduType.Int8 => sbyte.MinValue,
            KuduType.Int16 => short.MinValue,
            KuduType.Int32 => int.MinValue,
            KuduType.Date => EpochTime.MinDateValue,
            KuduType.Int64 => long.MinValue,
            KuduType.UnixtimeMicros => long.MinValue,
            _ => throw new Exception()
        };
    }

    internal static long MaxIntValue(KuduType type)
    {
        return type switch
        {
            KuduType.Int8 => sbyte.MaxValue,
            KuduType.Int16 => short.MaxValue,
            KuduType.Int32 => int.MaxValue,
            KuduType.Date => EpochTime.MaxDateValue,
            KuduType.Int64 => long.MaxValue,
            KuduType.UnixtimeMicros => long.MaxValue,
            _ => throw new Exception()
        };
    }

    private static byte[] GetBinary(KuduType type, long value)
    {
        return type switch
        {
            KuduType.Int8 => KuduEncoder.EncodeInt8((sbyte)value),
            KuduType.Int16 => KuduEncoder.EncodeInt16((short)value),
            KuduType.Int32 => KuduEncoder.EncodeInt32((int)value),
            KuduType.Date => KuduEncoder.EncodeInt32((int)value),
            KuduType.Decimal32 => KuduEncoder.EncodeInt32((int)value),
            KuduType.Int64 => KuduEncoder.EncodeInt64(value),
            KuduType.UnixtimeMicros => KuduEncoder.EncodeInt64(value),
            KuduType.Decimal64 => KuduEncoder.EncodeInt64(value),
            _ => throw new Exception()
        };
    }

    /// <summary>
    /// Compares two bounds based on the type of the column.
    /// </summary>
    /// <param name="column">The column which the values belong to.</param>
    /// <param name="a">The first serialized value.</param>
    /// <param name="b">The second serialized value.</param>
    private static int Compare(ColumnSchema column, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        switch (column.Type)
        {
            case KuduType.Bool:
                return KuduEncoder.DecodeBool(a).CompareTo(KuduEncoder.DecodeBool(b));
            case KuduType.Int8:
                return KuduEncoder.DecodeInt8(a).CompareTo(KuduEncoder.DecodeInt8(b));
            case KuduType.Int16:
                return KuduEncoder.DecodeInt16(a).CompareTo(KuduEncoder.DecodeInt16(b));
            case KuduType.Int32:
            case KuduType.Date:
            case KuduType.Decimal32:
                return KuduEncoder.DecodeInt32(a).CompareTo(KuduEncoder.DecodeInt32(b));
            case KuduType.Int64:
            case KuduType.UnixtimeMicros:
            case KuduType.Decimal64:
                return KuduEncoder.DecodeInt64(a).CompareTo(KuduEncoder.DecodeInt64(b));
            case KuduType.Float:
                return KuduEncoder.DecodeFloat(a).CompareTo(KuduEncoder.DecodeFloat(b));
            case KuduType.Double:
                return KuduEncoder.DecodeDouble(a).CompareTo(KuduEncoder.DecodeDouble(b));
            case KuduType.String:
            case KuduType.Binary:
            case KuduType.Varchar:
                return a.SequenceCompareTo(b);
            case KuduType.Decimal128:
                return KuduEncoder.DecodeInt128(a).CompareTo(KuduEncoder.DecodeInt128(b));
            default:
                throw new Exception($"Unknown column type {column.Type}");
        }
    }

    /// <summary>
    /// Returns true if increment(a) == b.
    /// </summary>
    /// <param name="a">The value which would be incremented.</param>
    /// <param name="b">The target value.</param>
    private bool AreConsecutive(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        switch (Column.Type)
        {
            case KuduType.Bool: return false;
            case KuduType.Int8:
                {
                    sbyte m = KuduEncoder.DecodeInt8(a);
                    sbyte n = KuduEncoder.DecodeInt8(b);
                    return m < n && m + 1 == n;
                }
            case KuduType.Int16:
                {
                    short m = KuduEncoder.DecodeInt16(a);
                    short n = KuduEncoder.DecodeInt16(b);
                    return m < n && m + 1 == n;
                }
            case KuduType.Int32:
            case KuduType.Date:
            case KuduType.Decimal32:
                {
                    int m = KuduEncoder.DecodeInt32(a);
                    int n = KuduEncoder.DecodeInt32(b);
                    return m < n && m + 1 == n;
                }
            case KuduType.Int64:
            case KuduType.UnixtimeMicros:
            case KuduType.Decimal64:
                {
                    long m = KuduEncoder.DecodeInt64(a);
                    long n = KuduEncoder.DecodeInt64(b);
                    return m < n && m + 1 == n;
                }
            case KuduType.Float:
                {
                    float m = KuduEncoder.DecodeFloat(a);
                    float n = KuduEncoder.DecodeFloat(b);
                    return m < n && m.NextUp() == n;
                }
            case KuduType.Double:
                {
                    double m = KuduEncoder.DecodeDouble(a);
                    double n = KuduEncoder.DecodeDouble(b);
                    return m < n && m.NextUp() == n;
                }
            case KuduType.String:
            case KuduType.Binary:
            case KuduType.Varchar:
                {
                    if (a.Length + 1 != b.Length || b[a.Length] != 0)
                    {
                        return false;
                    }

                    return a.SequenceEqual(b.Slice(0, a.Length));
                }
            case KuduType.Decimal128:
                {
                    Int128 m = KuduEncoder.DecodeInt128(a);
                    Int128 n = KuduEncoder.DecodeInt128(b);

                    return m < n && (m + 1) == n;
                }
            default:
                throw new Exception($"Unknown column type {Column.Type}");
        }
    }

    /// <summary>
    /// Builds an IN list predicate from a collection of raw values. The collection
    /// must be sorted and deduplicated.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <param name="values">The IN list values.</param>
    private static KuduPredicate BuildInList(ColumnSchema column, SortedSet<byte[]> values)
    {
        var numPredicates = values.Count;

        // IN (true, false) predicates can be simplified to IS NOT NULL.
        if (column.Type == KuduType.Bool && numPredicates > 1)
        {
            return NewIsNotNullPredicate(column);
        }

        return numPredicates switch
        {
            0 => None(column),
            1 => EqualPredicate(column, values.Min!),
            _ => new KuduPredicate(column, values)
        };
    }

    /// <summary>
    /// Check if this Range predicate contains the value.
    /// </summary>
    /// <param name="value">The value to check.</param>
    private bool RangeContains(byte[] value)
    {
        return (Lower == null || Compare(Column, value, Lower) >= 0) &&
               (Upper == null || Compare(Column, value, Upper) < 0);
    }

    private bool CheckValueInBloomFilter(ReadOnlySpan<byte> value)
    {
        var bloomFilters = BloomFilters!;

        foreach (var bloomFilter in bloomFilters)
        {
            if (!bloomFilter.FindBinary(value))
                return false;
        }

        return true;
    }

    private (byte[]? Lower, byte[]? Upper) MergeRange(KuduPredicate other)
    {
        // Set the lower bound to the larger of the two.
        byte[]? newLower = other.Lower == null ||
            (Lower != null && Compare(Column, Lower, other.Lower) >= 0) ? Lower : other.Lower;

        // Set the upper bound to the smaller of the two.
        byte[]? newUpper = other.Upper == null ||
            (Upper != null && Compare(Column, Upper, other.Upper) <= 0) ? Upper : other.Upper;

        return (newLower, newUpper);
    }

    public static KuduPredicate FromPb(KuduSchema schema, ColumnPredicatePB pb)
    {
        ColumnSchema column = schema.GetColumn(pb.Column);

        if (pb.Equality != null)
        {
            var equality = pb.Equality.HasValue ? pb.Equality.Value.ToByteArray() : null;
            return new KuduPredicate(PredicateType.Equality, column, equality, null);
        }
        else if (pb.Range != null)
        {
            var range = pb.Range;
            var lower = range.HasLower ? range.Lower.ToByteArray() : null;
            var upper = range.HasUpper ? range.Upper.ToByteArray() : null;

            return new KuduPredicate(PredicateType.Range, column, lower, upper);
        }
        else if (pb.IsNotNull != null)
        {
            return NewIsNotNullPredicate(column);
        }
        else if (pb.IsNull != null)
        {
            return NewIsNullPredicate(column);
        }
        else if (pb.InList != null)
        {
            var values = new SortedSet<byte[]>(new PredicateComparer(column));

            foreach (var value in pb.InList.Values)
                values.Add(value.ToByteArray());

            return BuildInList(column, values);
        }
        else if (pb.InBloomFilter != null)
        {
            var inBloomFilter = pb.InBloomFilter;
            var lower = inBloomFilter.HasLower ? inBloomFilter.Lower.ToByteArray() : null;
            var upper = inBloomFilter.HasUpper ? inBloomFilter.Upper.ToByteArray() : null;

            var numBloomFilters = inBloomFilter.BloomFilters.Count;

            if (numBloomFilters <= 0)
            {
                throw new ArgumentException("Invalid bloom filter predicate on column " +
                    $"{column.Name}. No bloom filters supplied");
            }

            var bloomFilters = new List<KuduBloomFilter>(numBloomFilters);

            foreach (var bloomFilterPb in inBloomFilter.BloomFilters)
            {
                var blockBloomFilter = new BlockBloomFilter(
                    bloomFilterPb.LogSpaceBytes,
                    bloomFilterPb.BloomData.ToByteArray(),
                    bloomFilterPb.AlwaysFalse);

                var bloomFilter = new KuduBloomFilter(
                    blockBloomFilter,
                    bloomFilterPb.HashSeed,
                    column);

                bloomFilters.Add(bloomFilter);
            }

            return NewInBloomFilterPredicate(bloomFilters, lower, upper);
        }
        else
        {
            throw new ArgumentException($"Unknown predicate type for column {column.Name}");
        }
    }

    private sealed class PredicateComparer : IComparer<byte[]>
    {
        private readonly ColumnSchema _column;

        public PredicateComparer(ColumnSchema column)
        {
            _column = column;
        }

        public int Compare(byte[]? x, byte[]? y)
        {
            return KuduPredicate.Compare(_column, x!, y!);
        }
    }
}
