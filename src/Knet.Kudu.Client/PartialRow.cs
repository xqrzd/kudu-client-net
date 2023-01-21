using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client;

public class PartialRow
{
    public KuduSchema Schema { get; }

    private readonly byte[] _rowAlloc;
    private readonly int _headerSize;
    private readonly int _nullOffset;

    private readonly byte[]?[] _varLengthData;

    public PartialRow(KuduSchema schema)
    {
        Schema = schema;

        var columnBitmapSize = KuduEncoder.BitsToBytes(schema.Columns.Count);
        var headerSize = columnBitmapSize;
        if (schema.HasNullableColumns)
        {
            // nullsBitSet is the same size as the columnBitSet.
            // Bits for non-nullable columns are ignored.
            headerSize += columnBitmapSize;
            _nullOffset = columnBitmapSize;
        }

        _rowAlloc = new byte[headerSize + schema.RowAllocSize];

        _headerSize = headerSize;
        _varLengthData = new byte[schema.VarLengthColumnCount][];
    }

    /// <summary>
    /// Creates a new partial row by deep-copying the data-fields of the
    /// provided partial row.
    /// </summary>
    /// <param name="row">The partial row to copy.</param>
    internal PartialRow(PartialRow row)
    {
        Schema = row.Schema;
        _rowAlloc = CloneArray(row._rowAlloc);
        _headerSize = row._headerSize;
        _nullOffset = row._nullOffset;
        _varLengthData = new byte[row._varLengthData.Length][];
        for (int i = 0; i < _varLengthData.Length; i++)
            _varLengthData[i] = CloneArray(row._varLengthData[i]);
    }

    public void WriteTo(
        Span<byte> rowDestination,
        Span<byte> indirectDestination,
        int indirectDataStart,
        out int rowBytesWritten,
        out int indirectBytesWritten)
    {
        var schema = Schema;
        ReadOnlySpan<byte> rowAlloc = _rowAlloc;

        // Write the header. This includes,
        // - Column set bitmap
        // - Nullset bitmap
        rowAlloc.Slice(0, _headerSize).CopyTo(rowDestination);

        // Advance buffers.
        rowAlloc = rowAlloc.Slice(_headerSize);
        rowDestination = rowDestination.Slice(_headerSize);
        rowBytesWritten = _headerSize;

        int varLengthOffset = indirectDataStart;
        int numColumns = schema.Columns.Count;

        indirectBytesWritten = 0;

        for (int i = 0; i < numColumns; i++)
        {
            var column = schema.GetColumn(i);
            var isFixed = column.IsFixedSize;
            var size = column.Size;

            if (IsSet(i) && !IsSetToNull(i))
            {
                if (column.IsFixedSize)
                {
                    var data = rowAlloc.Slice(0, size);
                    data.CopyTo(rowDestination);
                }
                else
                {
                    var data = GetVarLengthColumn(i);
                    data.CopyTo(indirectDestination);
                    BinaryPrimitives.WriteInt64LittleEndian(rowDestination, varLengthOffset);
                    BinaryPrimitives.WriteInt64LittleEndian(rowDestination.Slice(8), data.Length);

                    indirectDestination = indirectDestination.Slice(data.Length);
                    varLengthOffset += data.Length;
                    indirectBytesWritten += data.Length;
                }

                rowDestination = rowDestination.Slice(size);
                rowBytesWritten += size;
            }

            if (isFixed)
            {
                // Advance rowAlloc even if it doesn't have a value.
                // It was pre-allocated to store space for every value.
                rowAlloc = rowAlloc.Slice(size);
            }
        }
    }

    private bool IsSet(int columnIndex) => _rowAlloc.GetBit(0, columnIndex);

    private void Set(int columnIndex) => _rowAlloc.SetBit(0, columnIndex);

    private bool IsSetToNull(int columnIndex) =>
        Schema.HasNullableColumns && _rowAlloc.GetBit(_nullOffset, columnIndex);

    private void SetToNull(int columnIndex) =>
        _rowAlloc.SetBit(_nullOffset, columnIndex);

    public void SetNull(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetNull(index);
    }

    public void SetNull(int columnIndex)
    {
        ColumnSchema column = Schema.GetColumn(columnIndex);

        if (!column.IsNullable)
            KuduTypeValidation.ThrowNotNullableException(column);

        Set(columnIndex);
        SetToNull(columnIndex);
    }

    public bool IsNull(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return IsNull(index);
    }

    public bool IsNull(int columnIndex)
    {
        return IsSetToNull(columnIndex);
    }

    public void SetBool(string columnName, bool value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetBool(index, value);
    }

    public void SetBool(int columnIndex, bool value)
    {
        CheckType(columnIndex, KuduType.Bool);
        WriteBool(columnIndex, value);
    }

    private void WriteBool(int columnIndex, bool value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 1);
        KuduEncoder.EncodeBool(span, value);
    }

    public bool GetBool(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetBool(index);
    }

    public bool GetBool(int columnIndex)
    {
        CheckType(columnIndex, KuduType.Bool);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 1);
        return KuduEncoder.DecodeBool(data);
    }

    public void SetByte(string columnName, byte value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetByte(index, value);
    }

    public void SetByte(int columnIndex, byte value)
    {
        CheckType(columnIndex, KuduType.Int8);
        WriteByte(columnIndex, value);
    }

    private void WriteByte(int columnIndex, byte value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 1);
        KuduEncoder.EncodeInteger(span, value);
    }

    public byte GetByte(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetByte(index);
    }

    public byte GetByte(int columnIndex)
    {
        CheckType(columnIndex, KuduType.Int8);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 1);
        return KuduEncoder.DecodeUInt8(data);
    }

    public void SetSByte(string columnName, sbyte value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetSByte(index, value);
    }

    public void SetSByte(int columnIndex, sbyte value)
    {
        CheckType(columnIndex, KuduType.Int8);
        WriteSByte(columnIndex, value);
    }

    private void WriteSByte(int columnIndex, sbyte value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 1);
        KuduEncoder.EncodeInteger(span, value);
    }

    public sbyte GetSByte(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetSByte(index);
    }

    public sbyte GetSByte(int columnIndex)
    {
        CheckType(columnIndex, KuduType.Int8);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 1);
        return KuduEncoder.DecodeInt8(data);
    }

    public void SetInt16(string columnName, short value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetInt16(index, value);
    }

    public void SetInt16(int columnIndex, short value)
    {
        CheckType(columnIndex, KuduType.Int16);
        WriteInt16(columnIndex, value);
    }

    private void WriteInt16(int columnIndex, short value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 2);
        KuduEncoder.EncodeInteger(span, value);
    }

    public short GetInt16(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetInt16(index);
    }

    public short GetInt16(int columnIndex)
    {
        CheckType(columnIndex, KuduType.Int16);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 2);
        return KuduEncoder.DecodeInt16(data);
    }

    public void SetInt32(string columnName, int value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetInt32(index, value);
    }

    public void SetInt32(int columnIndex, int value)
    {
        CheckType(columnIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);
        WriteInt32(columnIndex, value);
    }

    private void WriteInt32(int columnIndex, int value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 4);
        KuduEncoder.EncodeInteger(span, value);
    }

    public int GetInt32(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetInt32(index);
    }

    public int GetInt32(int columnIndex)
    {
        CheckType(columnIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 4);
        return KuduEncoder.DecodeInt32(data);
    }

    public void SetInt64(string columnName, long value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetInt64(index, value);
    }

    public void SetInt64(int columnIndex, long value)
    {
        CheckType(columnIndex, KuduTypeFlags.Int64 | KuduTypeFlags.UnixtimeMicros);
        WriteInt64(columnIndex, value);
    }

    private void WriteInt64(int columnIndex, long value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
        KuduEncoder.EncodeInteger(span, value);
    }

    public long GetInt64(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetInt64(index);
    }

    public long GetInt64(int columnIndex)
    {
        CheckType(columnIndex, KuduTypeFlags.Int64 | KuduTypeFlags.UnixtimeMicros);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 8);
        return KuduEncoder.DecodeInt64(data);
    }

    public void SetDateTime(string columnName, DateTime value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetDateTime(index, value);
    }

    public void SetDateTime(int columnIndex, DateTime value)
    {
        var column = Schema.GetColumn(columnIndex);
        var type = column.Type;

        if (type == KuduType.UnixtimeMicros)
        {
            WriteDateTime(columnIndex, value);
        }
        else if (type == KuduType.Date)
        {
            WriteDate(columnIndex, value);
        }
        else
        {
            KuduTypeValidation.ThrowException(column,
                KuduTypeFlags.UnixtimeMicros |
                KuduTypeFlags.Date);
        }
    }

    private void WriteDateTime(int columnIndex, DateTime value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
        KuduEncoder.EncodeDateTime(span, value);
    }

    private void WriteDate(int columnIndex, DateTime value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 4);
        KuduEncoder.EncodeDate(span, value);
    }

    public DateTime GetDateTime(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetDateTime(index);
    }

    public DateTime GetDateTime(int columnIndex)
    {
        var column = Schema.GetColumn(columnIndex);
        var type = column.Type;

        if (type == KuduType.UnixtimeMicros)
        {
            CheckValue(columnIndex);
            ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 8);
            return KuduEncoder.DecodeDateTime(data);
        }
        else if (type == KuduType.Date)
        {
            CheckValue(columnIndex);
            ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 4);
            return KuduEncoder.DecodeDate(data);
        }

        return KuduTypeValidation.ThrowException<DateTime>(column,
            KuduTypeFlags.UnixtimeMicros |
            KuduTypeFlags.Date);
    }

    public void SetFloat(string columnName, float value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetFloat(index, value);
    }

    public void SetFloat(int columnIndex, float value)
    {
        CheckType(columnIndex, KuduType.Float);
        WriteFloat(columnIndex, value);
    }

    private void WriteFloat(int columnIndex, float value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 4);
        KuduEncoder.EncodeFloat(span, value);
    }

    public float GetFloat(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetFloat(index);
    }

    public float GetFloat(int columnIndex)
    {
        CheckType(columnIndex, KuduType.Float);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 4);
        return KuduEncoder.DecodeFloat(data);
    }

    public void SetDouble(string columnName, double value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetDouble(index, value);
    }

    public void SetDouble(int columnIndex, double value)
    {
        CheckType(columnIndex, KuduType.Double);
        WriteDouble(columnIndex, value);
    }

    private void WriteDouble(int columnIndex, double value)
    {
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
        KuduEncoder.EncodeDouble(span, value);
    }

    public double GetDouble(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetDouble(index);
    }

    public double GetDouble(int columnIndex)
    {
        CheckType(columnIndex, KuduType.Double);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 8);
        return KuduEncoder.DecodeDouble(data);
    }

    public void SetDecimal(string columnName, decimal value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetDecimal(index, value);
    }

    public void SetDecimal(int columnIndex, decimal value)
    {
        var column = CheckType(columnIndex,
            KuduTypeFlags.Decimal32 |
            KuduTypeFlags.Decimal64 |
            KuduTypeFlags.Decimal128);

        var typeAttributes = column.TypeAttributes!;
        int precision = typeAttributes.Precision.GetValueOrDefault();
        int scale = typeAttributes.Scale.GetValueOrDefault();
        Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, column.Size);

        switch (column.Type)
        {
            case KuduType.Decimal32:
                KuduEncoder.EncodeDecimal32(span, value, precision, scale);
                break;
            case KuduType.Decimal64:
                KuduEncoder.EncodeDecimal64(span, value, precision, scale);
                break;
            default:
                KuduEncoder.EncodeDecimal128(span, value, precision, scale);
                break;
        }
    }

    public decimal GetDecimal(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetDecimal(index);
    }

    public decimal GetDecimal(int columnIndex)
    {
        ColumnSchema column = CheckType(columnIndex,
            KuduTypeFlags.Decimal32 |
            KuduTypeFlags.Decimal64 |
            KuduTypeFlags.Decimal128);

        int scale = column.TypeAttributes!.Scale.GetValueOrDefault();
        ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, column.Size);

        return column.Type switch
        {
            KuduType.Decimal32 => KuduEncoder.DecodeDecimal32(data, scale),
            KuduType.Decimal64 => KuduEncoder.DecodeDecimal64(data, scale),
            _ => KuduEncoder.DecodeDecimal128(data, scale),
        };
    }

    public void SetString(string columnName, string value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetString(index, value);
    }

    public void SetString(int columnIndex, string value)
    {
        var column = Schema.GetColumn(columnIndex);
        var type = column.Type;

        if (type == KuduType.String)
        {
        }
        else if (type == KuduType.Varchar)
        {
            int maxLength = column.TypeAttributes!.Length.GetValueOrDefault();

            if (value.Length > maxLength)
                value = value.Substring(0, maxLength);
        }
        else
        {
            KuduTypeValidation.ThrowException(column,
                KuduTypeFlags.String |
                KuduTypeFlags.Varchar);
        }

        WriteString(columnIndex, value);
    }

    private void WriteString(int columnIndex, string value)
    {
        var data = KuduEncoder.EncodeString(value);
        WriteBinary(columnIndex, data);
    }

    public string GetString(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetString(index);
    }

    public string GetString(int columnIndex)
    {
        CheckType(columnIndex, KuduTypeFlags.String | KuduTypeFlags.Varchar);
        CheckValue(columnIndex);
        ReadOnlySpan<byte> data = GetVarLengthColumn(columnIndex);
        return KuduEncoder.DecodeString(data);
    }

    public void SetBinary(string columnName, byte[] value)
    {
        int index = Schema.GetColumnIndex(columnName);
        SetBinary(index, value);
    }

    public void SetBinary(int columnIndex, byte[] value)
    {
        CheckType(columnIndex, KuduType.Binary);
        WriteBinary(columnIndex, value);
    }

    private void WriteBinary(int columnIndex, byte[] value)
    {
        Set(columnIndex);
        int varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
        _varLengthData[varLenColumnIndex] = value;
    }

    public byte[] GetBinary(string columnName)
    {
        int index = Schema.GetColumnIndex(columnName);
        return GetBinary(index);
    }

    public byte[] GetBinary(int columnIndex)
    {
        CheckType(columnIndex, KuduType.Binary);
        CheckValue(columnIndex);
        int varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
        return _varLengthData[varLenColumnIndex]!;
    }

    /// <summary>
    /// Sets the column to the provided raw value.
    /// </summary>
    /// <param name="index">The index of the column to set.</param>
    /// <param name="value">The raw value.</param>
    internal void SetRaw(int index, byte[] value)
    {
        ColumnSchema column = Schema.GetColumn(index);

        if (column.IsFixedSize)
        {
            if (value.Length != column.Size)
                throw new ArgumentOutOfRangeException(nameof(value));

            Span<byte> rowAlloc = GetSpanInRowAllocAndSetBitSet(index, value.Length);
            value.CopyTo(rowAlloc);
        }
        else
        {
            WriteBinary(index, value);
        }
    }

    /// <summary>
    /// Sets the column to the minimum possible value for the column's type.
    /// </summary>
    /// <param name="index">The index of the column to set to the minimum.</param>
    internal void SetMin(int index)
    {
        ColumnSchema column = Schema.GetColumn(index);
        KuduType type = column.Type;

        switch (type)
        {
            case KuduType.Bool:
                WriteBool(index, false);
                break;
            case KuduType.Int8:
                WriteSByte(index, sbyte.MinValue);
                break;
            case KuduType.Int16:
                WriteInt16(index, short.MinValue);
                break;
            case KuduType.Int32:
                WriteInt32(index, int.MinValue);
                break;
            case KuduType.Date:
                WriteInt32(index, EpochTime.MinDateValue);
                break;
            case KuduType.Int64:
            case KuduType.UnixtimeMicros:
                WriteInt64(index, long.MinValue);
                break;
            case KuduType.Float:
                WriteFloat(index, float.MinValue);
                break;
            case KuduType.Double:
                WriteDouble(index, double.MinValue);
                break;
            case KuduType.Decimal32:
                WriteInt32(index, DecimalUtil.MinDecimal32(
                    column.TypeAttributes!.Precision.GetValueOrDefault()));
                break;
            case KuduType.Decimal64:
                WriteInt64(index, DecimalUtil.MinDecimal64(
                    column.TypeAttributes!.Precision.GetValueOrDefault()));
                break;
            case KuduType.Decimal128:
                {
                    Int128 min = DecimalUtil.MinDecimal128(
                        column.TypeAttributes!.Precision.GetValueOrDefault());
                    Span<byte> span = GetSpanInRowAllocAndSetBitSet(index, 16);
                    KuduEncoder.EncodeInteger(span, min);
                    break;
                }
            case KuduType.String:
            case KuduType.Varchar:
                WriteString(index, string.Empty);
                break;
            case KuduType.Binary:
                WriteBinary(index, Array.Empty<byte>());
                break;
            default:
                throw new Exception($"Unsupported data type {type}");
        }
    }

    /// <summary>
    /// Increments the column at the given index, returning false if the
    /// value is already the maximum.
    /// </summary>
    /// <param name="index">The column index to increment.</param>
    internal bool IncrementColumn(int index)
    {
        if (!IsSet(index))
            throw new ArgumentException($"Column index {index} has not been set.");

        ColumnSchema column = Schema.GetColumn(index);

        if (column.IsFixedSize)
        {
            KuduType type = column.Type;
            Span<byte> data = GetRowAllocColumn(index, column.Size);

            switch (type)
            {
                case KuduType.Bool:
                    {
                        bool isFalse = data[0] == 0;
                        data[0] = 1;
                        return isFalse;
                    }
                case KuduType.Int8:
                    {
                        sbyte existing = KuduEncoder.DecodeInt8(data);
                        if (existing == sbyte.MaxValue)
                            return false;

                        KuduEncoder.EncodeInteger(data, (sbyte)(existing + 1));
                        return true;
                    }
                case KuduType.Int16:
                    {
                        short existing = KuduEncoder.DecodeInt16(data);
                        if (existing == short.MaxValue)
                            return false;

                        KuduEncoder.EncodeInteger(data, (short)(existing + 1));
                        return true;
                    }
                case KuduType.Int32:
                    {
                        int existing = KuduEncoder.DecodeInt32(data);
                        if (existing == int.MaxValue)
                            return false;

                        KuduEncoder.EncodeInteger(data, existing + 1);
                        return true;
                    }
                case KuduType.Date:
                    {
                        int existing = KuduEncoder.DecodeInt32(data);
                        if (existing == EpochTime.MaxDateValue)
                            return false;

                        KuduEncoder.EncodeInteger(data, existing + 1);
                        return true;
                    }
                case KuduType.Int64:
                case KuduType.UnixtimeMicros:
                    {
                        long existing = KuduEncoder.DecodeInt64(data);
                        if (existing == long.MaxValue)
                            return false;

                        KuduEncoder.EncodeInteger(data, existing + 1);
                        return true;
                    }
                case KuduType.Float:
                    {
                        float existing = KuduEncoder.DecodeFloat(data);
                        float incremented = existing.NextUp();
                        if (existing == incremented)
                            return false;

                        KuduEncoder.EncodeFloat(data, incremented);
                        return true;
                    }
                case KuduType.Double:
                    {
                        double existing = KuduEncoder.DecodeDouble(data);
                        double incremented = existing.NextUp();
                        if (existing == incremented)
                            return false;

                        KuduEncoder.EncodeDouble(data, incremented);
                        return true;
                    }
                case KuduType.Decimal32:
                    {
                        int existing = KuduEncoder.DecodeInt32(data);
                        int precision = column.TypeAttributes!.Precision.GetValueOrDefault();
                        if (existing == DecimalUtil.MaxDecimal32(precision))
                            return false;

                        KuduEncoder.EncodeInteger(data, existing + 1);
                        return true;
                    }
                case KuduType.Decimal64:
                    {
                        long existing = KuduEncoder.DecodeInt64(data);
                        int precision = column.TypeAttributes!.Precision.GetValueOrDefault();
                        if (existing == DecimalUtil.MaxDecimal64(precision))
                            return false;

                        KuduEncoder.EncodeInteger(data, existing + 1);
                        return true;
                    }
                case KuduType.Decimal128:
                    {
                        Int128 existing = KuduEncoder.DecodeInt128(data);
                        int precision = column.TypeAttributes!.Precision.GetValueOrDefault();
                        if (existing == DecimalUtil.MaxDecimal128(precision))
                            return false;

                        KuduEncoder.EncodeInteger(data, existing + 1);
                        return true;
                    }
                default:
                    throw new Exception($"Unsupported data type {type}");
            }
        }
        else
        {
            // Column is either string, binary, or varchar.
            ReadOnlySpan<byte> data = GetVarLengthColumn(index);
            var incremented = new byte[data.Length + 1];
            data.CopyTo(incremented);
            WriteBinary(index, incremented);
            return true;
        }
    }

    internal void CalculateSize(out int rowSize, out int indirectSize)
    {
        var schema = Schema;
        var numColumns = schema.Columns.Count;
        var localRowSize = _headerSize;
        var localIndirectSize = 0;

        for (int i = 0; i < numColumns; i++)
        {
            if (IsSet(i) && !IsSetToNull(i))
            {
                var column = schema.GetColumn(i);
                localRowSize += column.Size;

                if (!column.IsFixedSize)
                {
                    int varLenColumnIndex = Schema.GetColumnOffset(i);
                    var data = _varLengthData[varLenColumnIndex];
                    localIndirectSize += data!.Length;
                }
            }
        }

        rowSize = localRowSize;
        indirectSize = localIndirectSize;
    }

    private ColumnSchema CheckType(int columnIndex, KuduType type)
    {
        var columnSchema = Schema.GetColumn(columnIndex);
        KuduTypeValidation.ValidateColumnType(columnSchema, type);
        return columnSchema;
    }

    private ColumnSchema CheckType(int columnIndex, KuduTypeFlags typeFlags)
    {
        var columnSchema = Schema.GetColumn(columnIndex);
        KuduTypeValidation.ValidateColumnType(columnSchema, typeFlags);
        return columnSchema;
    }

    [DoesNotReturn]
    private void ThrowNotSetException(int columnIndex)
    {
        var column = Schema.GetColumn(columnIndex);
        throw new Exception($"Column {column} is not set");
    }

    [DoesNotReturn]
    private void ThrowNullException(int columnIndex)
    {
        var column = Schema.GetColumn(columnIndex);
        throw new Exception($"Column {column} is null");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckValue(int columnIndex)
    {
        if (!IsSet(columnIndex))
        {
            ThrowNotSetException(columnIndex);
        }

        if (IsNull(columnIndex))
        {
            ThrowNullException(columnIndex);
        }
    }

    private int GetPositionInRowAlloc(int columnIndex) =>
        Schema.GetColumnOffset(columnIndex) + _headerSize;

    private int GetPositionInRowAllocAndSetBitSet(int columnIndex)
    {
        Set(columnIndex);
        return GetPositionInRowAlloc(columnIndex);
    }

    private Span<byte> GetSpanInRowAllocAndSetBitSet(int columnIndex, int dataSize)
    {
        var position = GetPositionInRowAllocAndSetBitSet(columnIndex);
        return _rowAlloc.AsSpan(position, dataSize);
    }

    internal Span<byte> GetRowAllocColumn(int columnIndex, int dataSize)
    {
        var position = GetPositionInRowAlloc(columnIndex);
        return _rowAlloc.AsSpan(position, dataSize);
    }

    internal ReadOnlySpan<byte> GetVarLengthColumn(int columnIndex)
    {
        var varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
        return _varLengthData[varLenColumnIndex];
    }

    [return: NotNullIfNotNull("array")]
    private static byte[]? CloneArray(byte[]? array)
    {
        if (array is null)
            return null;

        var newArray = new byte[array.Length];
        array.CopyTo(newArray, 0);
        return newArray;
    }
}
