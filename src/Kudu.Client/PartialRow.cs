using System;
using System.Buffers.Binary;
using Kudu.Client.Builder;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public class PartialRow
    {
        public Schema Schema { get; }

        private readonly byte[] _rowAlloc;
        private readonly int _headerSize;
        private readonly int _nullOffset;

        private readonly byte[][] _varLengthData;

        // TODO: Move this to Operation.
        private readonly RowOperation? _rowOperation;

        public PartialRow(Schema schema, RowOperation? rowOperation = null)
        {
            Schema = schema;

            var columnBitmapSize = BitsToBytes(schema.ColumnCount);
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
            _rowOperation = rowOperation;
        }

        internal int RowSize => GetRowSize() + 1; // TODO: Remove this as part of RowOperation.

        internal int IndirectDataSize
        {
            get
            {
                int sum = 0;
                var varLengthData = _varLengthData;

                for (int i = 0; i < varLengthData.Length; i++)
                {
                    var buffer = varLengthData[0];

                    if (buffer != null)
                        sum += buffer.Length;
                }

                return sum;
            }
        }

        // TODO:
        //public int PrimaryKeySize { get; private set; }

        public void WriteTo(Span<byte> buffer, Span<byte> indirectData)
        {
            ReadOnlySpan<byte> rowAlloc = _rowAlloc.AsSpan();

            // Write the header. This includes,
            // 1) Row operation
            // 2) Column set bitmap
            // 3) Nullset bitmap
            if (_rowOperation.HasValue)
            {
                buffer[0] = (byte)_rowOperation.Value;
                buffer = buffer.Slice(1);
            }

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

        private bool IsSet(int columnIndex) => BitmapGet(0, columnIndex);

        private void Set(int columnIndex) => BitmapSet(0, columnIndex);

        private bool IsSetToNull(int columnIndex) =>
            Schema.HasNullableColumns ? BitmapGet(_nullOffset, columnIndex) : false;

        private void SetToNull(int columnIndex) => BitmapSet(_nullOffset, columnIndex);

        public void SetNull(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetNull(index);
        }

        public void SetNull(int columnIndex)
        {
            ColumnSchema column = Schema.GetColumn(columnIndex);

            if (!column.IsNullable)
                throw new ArgumentException($"{column.Name} cannot be set to null.");

            Set(columnIndex);
            SetToNull(columnIndex);
        }

        public void SetBool(string columnName, bool value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetBool(index, value);
        }

        public void SetBool(int columnIndex, bool value)
        {
            CheckColumn(columnIndex, DataType.Bool);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 1);
            KuduEncoder.EncodeBool(span, value);
        }

        public void SetByte(string columnName, byte value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetByte(index, value);
        }

        public void SetByte(int columnIndex, byte value)
        {
            SetSByte(columnIndex, (sbyte)value);
        }

        public void SetSByte(string columnName, sbyte value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetSByte(index, value);
        }

        public void SetSByte(int columnIndex, sbyte value)
        {
            CheckColumn(columnIndex, DataType.Int8);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 1);
            KuduEncoder.EncodeInt8(span, value);
        }

        public void SetInt16(string columnName, short value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetInt16(index, value);
        }

        public void SetInt16(int columnIndex, short value)
        {
            CheckFixedColumnSize(columnIndex, DataType.Int16, 2);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 2);
            KuduEncoder.EncodeInt16(span, value);
        }

        public void SetInt32(string columnName, int value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetInt32(index, value);
        }

        public void SetInt32(int columnIndex, int value)
        {
            CheckFixedColumnSize(columnIndex, DataType.Int32, 4);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 4);
            KuduEncoder.EncodeInt32(span, value);
        }

        public void SetInt64(string columnName, long value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetInt64(index, value);
        }

        public void SetInt64(int columnIndex, long value)
        {
            CheckFixedColumnSize(columnIndex, DataType.Int64, 8);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
            KuduEncoder.EncodeInt64(span, value);
        }

        public void SetDateTime(string columnName, DateTime value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetDateTime(index, value);
        }

        public void SetDateTime(int columnIndex, DateTime value)
        {
            CheckFixedColumnSize(columnIndex, DataType.UnixtimeMicros, 8);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
            KuduEncoder.EncodeDateTime(span, value);
        }

        public void SetFloat(string columnName, float value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetFloat(index, value);
        }

        public void SetFloat(int columnIndex, float value)
        {
            CheckColumn(columnIndex, DataType.Float);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 4);
            KuduEncoder.EncodeFloat(span, value);
        }

        public void SetDouble(string columnName, double value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetDouble(index, value);
        }

        public void SetDouble(int columnIndex, double value)
        {
            CheckColumn(columnIndex, DataType.Double);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
            KuduEncoder.EncodeDouble(span, value);
        }

        public void SetDecimal(string columnName, decimal value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetDecimal(index, value);
        }

        public void SetDecimal(int columnIndex, decimal value)
        {
            ColumnSchema column = Schema.GetColumn(columnIndex);
            int precision = column.TypeAttributes.Precision;
            int scale = column.TypeAttributes.Scale;
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, column.Size);

            switch (column.Type)
            {
                case DataType.Decimal32:
                    KuduEncoder.EncodeDecimal32(span, value, precision, scale);
                    break;
                case DataType.Decimal64:
                    KuduEncoder.EncodeDecimal64(span, value, precision, scale);
                    break;
                case DataType.Decimal128:
                    KuduEncoder.EncodeDecimal128(span, value, precision, scale);
                    break;
                default:
                    throw new ArgumentException($"Column {column.Name} is not a decimal.");
            }
        }

        public void SetString(string columnName, string value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetString(index, value);
        }

        public void SetString(int columnIndex, string value)
        {
            CheckColumn(columnIndex, DataType.String);
            var data = KuduEncoder.EncodeString(value);
            SetVarLengthData(columnIndex, data);
        }

        public void SetBinary(string columnName, byte[] value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetBinary(index, value);
        }

        public void SetBinary(int columnIndex, byte[] value)
        {
            CheckColumn(columnIndex, DataType.Binary);
            SetVarLengthData(columnIndex, value);
        }

        private void SetVarLengthData(int columnIndex, byte[] value)
        {
            Set(columnIndex);
            var varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
            _varLengthData[varLenColumnIndex] = value;
        }

        private void CheckColumn(int columnIndex, DataType type)
        {
            ColumnSchema column = Schema.GetColumn(columnIndex);

            if (column.Type != type)
                throw new ArgumentException(
                    $"Can't set {column.Name} ({column.Type}) to {type}");
        }

        private void CheckFixedColumnSize(int columnIndex, DataType type, int size)
        {
            ColumnSchema column = Schema.GetColumn(columnIndex);

            if (!column.IsFixedSize || column.Size != size)
                throw new ArgumentException(
                    $"Can't set {column.Name} ({column.Type}) to {type}");
        }

        private int GetPositionInRowAllocAndSetBitSet(int columnIndex)
        {
            Set(columnIndex);
            return Schema.GetColumnOffset(columnIndex);
        }

        private Span<byte> GetSpanInRowAllocAndSetBitSet(int columnIndex, int dataSize)
        {
            var position = _headerSize + GetPositionInRowAllocAndSetBitSet(columnIndex);
            return _rowAlloc.AsSpan(position, dataSize);
        }

        internal ReadOnlySpan<byte> GetRowAllocColumn(int columnIndex, int dataSize)
        {
            var position = _headerSize + Schema.GetColumnOffset(columnIndex);
            return _rowAlloc.AsSpan(position, dataSize);
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
