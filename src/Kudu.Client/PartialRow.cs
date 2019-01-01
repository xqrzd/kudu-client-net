using System;
using System.Buffers.Binary;
using System.Text;
using Kudu.Client.Builder;
using Kudu.Client.Util;

namespace Kudu.Client
{
    // TODO: Move this somewhere else.
    // TODO: A lot of cleanup needed here.
    public class PartialRow
    {
        public Schema Schema { get; }

        private readonly byte[] _rowAlloc;
        private readonly int _headerSize;
        private readonly int _nullOffset;

        private readonly byte[][] _varLengthData;

        public PartialRow(Schema schema, RowOperation type)
        {
            Schema = schema;

            var columnBitmapSize = BitsToBytes(schema.ColumnCount);
            var headerSize = 1 + columnBitmapSize;
            if (schema.HasNullableColumns)
                // nullsBitSet is the same size as the columnBitSet.
                headerSize += columnBitmapSize;

            _rowAlloc = new byte[headerSize + schema.RowAllocSize];
            _rowAlloc[0] = (byte)type;

            if (schema.HasNullableColumns)
                _nullOffset = 1 + columnBitmapSize;

            _headerSize = headerSize;
            _varLengthData = new byte[schema.VarLengthColumnCount][];
        }

        public int RowSize => GetRowSize();

        public int IndirectDataSize { get; private set; }

        // TODO:
        //public int PrimaryKeySize { get; private set; }

        public void WriteTo(Span<byte> buffer, Span<byte> indirectData)
        {
            ReadOnlySpan<byte> rowAlloc = _rowAlloc.AsSpan();

            // Write the header. This includes,
            // 1) Row operation
            // 2) Column set bitmap
            // 3) Nullset bitmap
            rowAlloc.Slice(0, _headerSize).CopyTo(buffer);

            // Advance buffers.
            rowAlloc = rowAlloc.Slice(_headerSize);
            buffer = buffer.Slice(_headerSize);

            int varLengthOffset = 0;

            for (int i = 0; i < Schema.ColumnCount; i++)
            {
                if (IsSet(i) && !IsSetToNull(i))
                {
                    var column = Schema.GetColumn(i);
                    var size = column.Size;
                    var type = column.Type;

                    if (type == DataType.String || type == DataType.Binary)
                    {
                        var data = GetVarLengthColumn(i);
                        data.CopyTo(indirectData);
                        BinaryPrimitives.WriteInt64LittleEndian(buffer, varLengthOffset);
                        BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(8), data.Length);

                        indirectData = indirectData.Slice(data.Length);
                        varLengthOffset += data.Length;
                    }
                    else
                    {
                        var data = rowAlloc.Slice(0, size);
                        data.CopyTo(buffer);

                        rowAlloc = rowAlloc.Slice(size);
                    }

                    // Advance RowAlloc buffer only if we wrote that column.
                    buffer = buffer.Slice(size);
                }
            }
        }

        private bool IsSet(int columnIndex) => BitmapGet(1, columnIndex);

        private void Set(int columnIndex) => BitmapSet(1, columnIndex);

        private bool IsSetToNull(int columnIndex) =>
            Schema.HasNullableColumns ? BitmapGet(_nullOffset, columnIndex) : false;

        private void SetToNull(int columnIndex) => BitmapSet(_nullOffset, columnIndex);

        public void SetNull(int columnIndex)
        {
            Set(columnIndex);
            SetToNull(columnIndex);
        }

        public void SetBool(int columnIndex, bool value)
        {
            SetByte(columnIndex, (byte)(value ? 1 : 0));
        }

        public void SetSByte(int columnIndex, sbyte value)
        {
            SetByte(columnIndex, (byte)value);
        }

        public void SetByte(int columnIndex, byte value)
        {
            Set(columnIndex);
            _rowAlloc[Schema.GetColumnOffset(columnIndex)] = value;
        }

        public void SetShort(int columnIndex, short value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteInt16LittleEndian(span, value);
        }

        public void SetUShort(int columnIndex, ushort value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteUInt16LittleEndian(span, value);
        }

        public void SetInt(int columnIndex, int value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteInt32LittleEndian(span, value);
        }

        public void SetUInt(int columnIndex, uint value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteUInt32LittleEndian(span, value);
        }

        public void SetLong(int columnIndex, long value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteInt64LittleEndian(span, value);
        }

        public void SetULong(int columnIndex, ulong value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteUInt64LittleEndian(span, value);
        }

        public void SetDateTime(int columnIndex, DateTime value)
        {
            var micros = EpochTime.ToUnixEpochMicros(value);
            SetLong(columnIndex, micros);
        }

        public void SetFloat(int columnIndex, float value)
        {
            var intValue = value.AsInt();
            SetInt(columnIndex, intValue);
        }

        public void SetDouble(int columnIndex, double value)
        {
            var longValue = value.AsLong();
            SetLong(columnIndex, longValue);
        }

        public void SetDecimal(int columnIndex, decimal value)
        {
            var column = Schema.GetColumn(columnIndex);
            var size = column.Size;
            var precision = column.TypeAttributes.Precision;
            var scale = column.TypeAttributes.Scale;

            switch (size)
            {
                case DecimalUtil.Decimal32Size:
                    {
                        var encodedValue = DecimalUtil.EncodeDecimal32(value, precision, scale);
                        SetInt(columnIndex, encodedValue);
                        break;
                    }
                case DecimalUtil.Decimal64Size:
                    {
                        var encodedValue = DecimalUtil.EncodeDecimal64(value, precision, scale);
                        SetLong(columnIndex, encodedValue);
                        break;
                    }
                case DecimalUtil.Decimal128Size:
                    {
                        var encodedValue = DecimalUtil.EncodeDecimal128(value, precision, scale);
                        var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
                        encodedValue.TryWriteBytes(span, out var written);

                        if (encodedValue.Sign == -1)
                        {
                            // TODO: Use C# 8 range here: written..^0
                            var slice = span.Slice(written, 16 - written);
                            slice.Fill(0xff);
                        }

                        break;
                    }
            }
        }

        public void SetString(int columnIndex, string value)
        {
            var data = Encoding.UTF8.GetBytes(value);
            SetBinary(columnIndex, data);
        }

        public void SetBinary(int columnIndex, byte[] value)
        {
            Set(columnIndex);

            var varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
            _varLengthData[varLenColumnIndex] = value;
            IndirectDataSize += value.Length;
        }

        private int GetPositionInRowAllocAndSetBitSet(int columnIndex)
        {
            Set(columnIndex);
            return Schema.GetColumnOffset(columnIndex);
        }

        private Span<byte> GetSpanInRowAllocAndSetBitSet(int columnIndex)
        {
            var position = _headerSize + GetPositionInRowAllocAndSetBitSet(columnIndex);
            // TODO: Include length here, so the span is the correct size.
            return _rowAlloc.AsSpan(position);
        }

        internal ReadOnlySpan<byte> GetRowAllocColumn(int columnIndex)
        {
            var position = _headerSize + Schema.GetColumnOffset(columnIndex);
            return _rowAlloc.AsSpan(position);
        }

        internal ReadOnlySpan<byte> GetVarLengthColumn(int columnIndex)
        {
            var varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
            return _varLengthData[varLenColumnIndex];
        }

        private int GetRowSize()
        {
            var size = _headerSize;

            for (int i = 0; i < Schema.ColumnCount; i++)
            {
                if (IsSet(i) && !IsSetToNull(i))
                {
                    var column = Schema.GetColumn(i);
                    size += column.Size;
                }
            }

            return size;
        }

        private void BitmapSet(int offset, int index)
        {
            _rowAlloc[offset + (index / 8)] |= (byte)(1 << (index % 8));
        }

        private bool BitmapGet(int offset, int index)
        {
            return (_rowAlloc[offset + (index / 8)] & (1 << (index % 8))) != 0;
        }

        private static int BitsToBytes(int bits) => (bits + 7) / 8;
    }
}
