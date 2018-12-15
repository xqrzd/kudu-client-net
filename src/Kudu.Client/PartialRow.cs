using System;
using System.Buffers.Binary;
using System.Text;
using Kudu.Client.Protocol;

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

        private readonly object[] _varLengthData;

        public PartialRow(Schema schema, RowOperationsPB.Type type)
        {
            Schema = schema;

            var columnSize = BitsToBytes(schema.ColumnCount);
            var headerSize = 1 + columnSize;
            if (schema.HasNullableColumns)
                headerSize += columnSize;

            _rowAlloc = new byte[headerSize + schema.RowSize];
            _rowAlloc[0] = (byte)type;

            if (schema.HasNullableColumns)
                _nullOffset = 1 + columnSize;

            _headerSize = headerSize;
            _varLengthData = new object[schema.ColumnCount];
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

                    if (type == DataTypePB.String)
                    {
                        var str = _varLengthData[i] as string;
                        var len = Encoding.UTF8.GetBytes(str, indirectData);
                        indirectData = indirectData.Slice(len);
                        BinaryPrimitives.WriteInt64LittleEndian(buffer, varLengthOffset);
                        BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(8), len);
                        varLengthOffset += len;
                    }
                    else if (type == DataTypePB.Binary)
                    {
                        var bin = _varLengthData[i] as byte[];
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

        private bool IsSet(int columnIndex) => BitMapGet(1, columnIndex);

        private void Set(int columnIndex) => BitMapSet(1, columnIndex);

        private bool IsSetToNull(int columnIndex) =>
            Schema.HasNullableColumns ? BitMapGet(_nullOffset, columnIndex) : false;

        private void SetToNull(int columnIndex) => BitMapSet(_nullOffset, columnIndex);

        public void SetBool(int columnIndex, bool value)
        {
            Set(columnIndex);
            _rowAlloc[Schema.GetColumnOffset(columnIndex)] = (byte)(value ? 1 : 0);
        }

        public void SetNull(int columnIndex)
        {
            Set(columnIndex);
            SetToNull(columnIndex);
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
            Set(columnIndex);

            // C# strings are unicode (UTF-16).
            // UTF-8 length may be different from string length.
            var length = Encoding.UTF8.GetByteCount(value);

            _varLengthData[columnIndex] = value;
            IndirectDataSize += length;
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

        internal Span<byte> GetSpanInRowAlloc(int columnIndex)
        {
            var position = _headerSize + Schema.GetColumnOffset(columnIndex);
            return _rowAlloc.AsSpan(position);
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

        private void BitMapSet(int offset, int index)
        {
            _rowAlloc[offset + (index / 8)] |= (byte)(1 << (index % 8));
        }

        private bool BitMapGet(int offset, int index)
        {
            return (_rowAlloc[offset + (index / 8)] & (1 << (index % 8))) != 0;
        }

        private static int BitsToBytes(int bits) => (bits + 7) / 8;
    }
}
