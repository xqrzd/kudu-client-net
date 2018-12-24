using System;
using System.Buffers.Binary;
using System.Text;
using Kudu.Client.Builder;

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
            var varLengthOffset = 0;
            // Write the header.
            _rowAlloc.AsSpan(0, _headerSize).CopyTo(buffer);
            buffer = buffer.Slice(_headerSize);

            var position = _headerSize;

            for (int i = 0; i < Schema.ColumnCount; i++)
            {
                var length = Schema.GetColumnSize(i);
                if (IsSet(i) && !IsSetToNull(i))
                {
                    var type = Schema.GetColumnType(i);

                    if (type == DataType.String || type == DataType.Binary)
                    {
                        var offset = Schema.GetColumnOffset(i);
                        var bin = _varLengthData[offset];
                        bin.AsSpan().CopyTo(indirectData);
                        indirectData = indirectData.Slice(bin.Length);
                        BinaryPrimitives.WriteInt64LittleEndian(buffer, varLengthOffset);
                        BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(8), bin.Length);
                        varLengthOffset += bin.Length;
                    }
                    else
                    {
                        _rowAlloc.AsSpan(position, length).CopyTo(buffer);
                        buffer = buffer.Slice(length);
                    }
                }

                position += length;
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
            Set(columnIndex);
            _rowAlloc[Schema.GetColumnOffset(columnIndex)] = (byte)(value ? 1 : 0);
        }

        public void SetShort(int columnIndex, short value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteInt16LittleEndian(span, value);
        }

        public void SetInt(int columnIndex, int value)
        {
            var span = GetSpanInRowAllocAndSetBitSet(columnIndex);
            BinaryPrimitives.WriteInt32LittleEndian(span, value);
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
            return _rowAlloc.AsSpan(position);
        }

        internal ReadOnlySpan<byte> GetSpanInRowAlloc(int columnIndex)
        {
            var position = _headerSize + Schema.GetColumnOffset(columnIndex);
            return _rowAlloc.AsSpan(position);
        }

        internal ReadOnlySpan<byte> GetSpanInVarLenData(int columnIndex)
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
                    size += Schema.GetColumnSize(i);
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
