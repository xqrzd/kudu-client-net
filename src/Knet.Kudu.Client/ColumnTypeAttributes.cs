using System;

namespace Knet.Kudu.Client;

public sealed class ColumnTypeAttributes : IEquatable<ColumnTypeAttributes>
{
    public int? Precision { get; }

    public int? Scale { get; }

    public int? Length { get; }

    public ColumnTypeAttributes(int? precision, int? scale, int? length)
    {
        Precision = precision;
        Scale = scale;
        Length = length;
    }

    /// <summary>
    /// Return a string representation appropriate for `type`.
    /// This is meant to be postfixed to the name of a primitive type to
    /// describe the full type, e.g. decimal(10, 4).
    /// </summary>
    /// <param name="type">The data type.</param>
    public string ToStringForType(KuduType type)
    {
        switch (type)
        {
            case KuduType.Decimal32:
            case KuduType.Decimal64:
            case KuduType.Decimal128:
                return $"({Precision}, {Scale})";
            case KuduType.Varchar:
                return $"({Length})";
            default:
                return "";
        }
    }

    public bool Equals(ColumnTypeAttributes? other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return
            Precision == other.Precision &&
            Scale == other.Scale &&
            Length == other.Length;
    }

    public override bool Equals(object? obj) => Equals(obj as ColumnTypeAttributes);

    public override int GetHashCode() => HashCode.Combine(Precision, Scale, Length);

    public static bool operator ==(ColumnTypeAttributes? lhs, ColumnTypeAttributes? rhs)
    {
        if (lhs is null)
        {
            return rhs is null;
        }

        return lhs.Equals(rhs);
    }

    public static bool operator !=(ColumnTypeAttributes? lhs, ColumnTypeAttributes? rhs) => !(lhs == rhs);

    public static ColumnTypeAttributes NewDecimalAttributes(
        int precision, int scale)
    {
        return new ColumnTypeAttributes(precision, scale, null);
    }

    public static ColumnTypeAttributes NewVarcharAttributes(int length)
    {
        return new ColumnTypeAttributes(null, null, length);
    }
}
