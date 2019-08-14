using System;
using System.Buffers.Binary;
using System.Numerics;
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

            var columnBitmapSize = BitsToBytes(schema.Columns.Count);
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

        /// <summary>
        /// Creates a new partial row by deep-copying the data-fields of the provided partial row.
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
            _rowOperation = row._rowOperation;
        }

        internal int RowSize => GetRowSize() + 1; // TODO: Remove this as part of RowOperation.

        internal int IndirectDataSize
        {
            get
            {
                int length = 0;
                var varLengthData = _varLengthData;

                for (int i = 0; i < varLengthData.Length; i++)
                {
                    var buffer = varLengthData[0];

                    if (buffer != null)
                        length += buffer.Length;
                }

                return length;
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

            for (int i = 0; i < Schema.Columns.Count; i++)
            {
                if (IsSet(i) && !IsSetToNull(i))
                {
                    var column = Schema.GetColumn(i);
                    var size = column.Size;
                    var type = column.Type;

                    if (type == KuduType.String || type == KuduType.Binary)
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

        public bool IsNull(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return IsNull(index);
        }

        public bool IsNull(int columnIndex)
        {
            ColumnSchema column = Schema.GetColumn(columnIndex);
            return column.IsNullable && IsSetToNull(columnIndex);
        }

        public void SetBool(string columnName, bool value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetBool(index, value);
        }

        public void SetBool(int columnIndex, bool value)
        {
            CheckColumn(columnIndex, KuduType.Bool);
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
            CheckColumn(columnIndex, KuduType.Bool);
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
            SetSByte(columnIndex, (sbyte)value);
        }

        public byte GetByte(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetByte(index);
        }

        public byte GetByte(int columnIndex)
        {
            return (byte)GetSByte(columnIndex);
        }

        public void SetSByte(string columnName, sbyte value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetSByte(index, value);
        }

        public void SetSByte(int columnIndex, sbyte value)
        {
            CheckColumn(columnIndex, KuduType.Int8);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 1);
            KuduEncoder.EncodeInt8(span, value);
        }

        public sbyte GetSByte(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetSByte(index);
        }

        public sbyte GetSByte(int columnIndex)
        {
            CheckColumn(columnIndex, KuduType.Int8);
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
            CheckFixedColumnSize(columnIndex, KuduType.Int16, 2);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 2);
            KuduEncoder.EncodeInt16(span, value);
        }

        public short GetInt16(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetInt16(index);
        }

        public short GetInt16(int columnIndex)
        {
            CheckFixedColumnSize(columnIndex, KuduType.Int16, 2);
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
            CheckFixedColumnSize(columnIndex, KuduType.Int32, 4);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 4);
            KuduEncoder.EncodeInt32(span, value);
        }

        public int GetInt32(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetInt32(index);
        }

        public int GetInt32(int columnIndex)
        {
            CheckFixedColumnSize(columnIndex, KuduType.Int32, 4);
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
            CheckFixedColumnSize(columnIndex, KuduType.Int64, 8);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
            KuduEncoder.EncodeInt64(span, value);
        }

        public long GetInt64(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetInt64(index);
        }

        public long GetInt64(int columnIndex)
        {
            CheckFixedColumnSize(columnIndex, KuduType.Int64, 8);
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
            CheckFixedColumnSize(columnIndex, KuduType.UnixtimeMicros, 8);
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, 8);
            KuduEncoder.EncodeDateTime(span, value);
        }

        public DateTime GetDateTime(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetDateTime(index);
        }

        public DateTime GetDateTime(int columnIndex)
        {
            CheckFixedColumnSize(columnIndex, KuduType.UnixtimeMicros, 8);
            CheckValue(columnIndex);
            ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, 8);
            return KuduEncoder.DecodeDateTime(data);
        }

        public void SetFloat(string columnName, float value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetFloat(index, value);
        }

        public void SetFloat(int columnIndex, float value)
        {
            CheckColumn(columnIndex, KuduType.Float);
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
            CheckColumn(columnIndex, KuduType.Float);
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
            CheckColumn(columnIndex, KuduType.Double);
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
            CheckColumn(columnIndex, KuduType.Double);
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
            ColumnSchema column = Schema.GetColumn(columnIndex);
            int precision = column.TypeAttributes.Precision;
            int scale = column.TypeAttributes.Scale;
            Span<byte> span = GetSpanInRowAllocAndSetBitSet(columnIndex, column.Size);

            switch (column.Type)
            {
                case KuduType.Decimal32:
                    KuduEncoder.EncodeDecimal32(span, value, precision, scale);
                    break;
                case KuduType.Decimal64:
                    KuduEncoder.EncodeDecimal64(span, value, precision, scale);
                    break;
                case KuduType.Decimal128:
                    KuduEncoder.EncodeDecimal128(span, value, precision, scale);
                    break;
                default:
                    throw new ArgumentException($"Column {column.Name} is not a decimal.");
            }
        }

        public decimal GetDecimal(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetDecimal(index);
        }

        public decimal GetDecimal(int columnIndex)
        {
            // TODO: Check type here.
            ColumnSchema column = Schema.GetColumn(columnIndex);
            int scale = column.TypeAttributes.Scale;
            ReadOnlySpan<byte> data = GetRowAllocColumn(columnIndex, column.Size);
            return KuduEncoder.DecodeDecimal(data, column.Type, scale);
        }

        public void SetString(string columnName, string value)
        {
            int index = Schema.GetColumnIndex(columnName);
            SetString(index, value);
        }

        public void SetString(int columnIndex, string value)
        {
            CheckColumn(columnIndex, KuduType.String);
            var data = KuduEncoder.EncodeString(value);
            SetVarLengthData(columnIndex, data);
        }

        public string GetString(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetString(index);
        }

        public string GetString(int columnIndex)
        {
            CheckColumn(columnIndex, KuduType.String);
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
            CheckColumn(columnIndex, KuduType.Binary);
            SetVarLengthData(columnIndex, value);
        }

        public byte[] GetBinary(string columnName)
        {
            int index = Schema.GetColumnIndex(columnName);
            return GetBinary(index);
        }

        public byte[] GetBinary(int columnIndex)
        {
            CheckColumn(columnIndex, KuduType.Binary);
            CheckValue(columnIndex);
            int varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
            return _varLengthData[varLenColumnIndex];
        }

        private void SetVarLengthData(int columnIndex, byte[] value)
        {
            Set(columnIndex);
            int varLenColumnIndex = Schema.GetColumnOffset(columnIndex);
            _varLengthData[varLenColumnIndex] = value;
        }

        /// <summary>
        /// Sets the column to the provided raw value.
        /// </summary>
        /// <param name="index">The index of the column to set.</param>
        /// <param name="value">The raw value.</param>
        internal void SetRaw(int index, byte[] value)
        {
            ColumnSchema column = Schema.GetColumn(index);
            KuduType type = column.Type;

            switch (type)
            {
                case KuduType.Bool:
                case KuduType.Int8:
                case KuduType.Int16:
                case KuduType.Int32:
                case KuduType.Int64:
                case KuduType.UnixtimeMicros:
                case KuduType.Float:
                case KuduType.Double:
                case KuduType.Decimal32:
                case KuduType.Decimal64:
                case KuduType.Decimal128:
                    {
                        if (value.Length != column.Size)
                            throw new ArgumentException();

                        Span<byte> rowAlloc = GetSpanInRowAllocAndSetBitSet(index, value.Length);
                        value.CopyTo(rowAlloc);

                        break;
                    }
                case KuduType.String:
                case KuduType.Binary:
                    {
                        SetVarLengthData(index, value);
                        break;
                    }
                default:
                    throw new Exception($"Unsupported data type {type}");
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
                    SetBool(index, false);
                    break;
                case KuduType.Int8:
                    SetSByte(index, sbyte.MinValue);
                    break;
                case KuduType.Int16:
                    SetInt16(index, short.MinValue);
                    break;
                case KuduType.Int32:
                    SetInt32(index, int.MinValue);
                    break;
                case KuduType.Int64:
                case KuduType.UnixtimeMicros:
                    SetInt64(index, long.MinValue);
                    break;
                case KuduType.Float:
                    SetFloat(index, float.MinValue);
                    break;
                case KuduType.Double:
                    SetDouble(index, double.MinValue);
                    break;
                case KuduType.Decimal32:
                    SetInt32(index, DecimalUtil.MinDecimal32(column.TypeAttributes.Precision));
                    break;
                case KuduType.Decimal64:
                    SetInt64(index, DecimalUtil.MinDecimal64(column.TypeAttributes.Precision));
                    break;
                case KuduType.Decimal128:
                    {
                        BigInteger min = DecimalUtil.MinDecimal128(column.TypeAttributes.Precision);
                        Span<byte> span = GetSpanInRowAllocAndSetBitSet(index, 16);
                        KuduEncoder.EncodeInt128(span, min);
                        break;
                    }
                case KuduType.String:
                    SetString(index, string.Empty);
                    break;
                case KuduType.Binary:
                    SetBinary(index, Array.Empty<byte>());
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

                            KuduEncoder.EncodeInt8(data, (sbyte)(existing + 1));
                            return true;
                        }
                    case KuduType.Int16:
                        {
                            short existing = KuduEncoder.DecodeInt16(data);
                            if (existing == short.MaxValue)
                                return false;

                            KuduEncoder.EncodeInt16(data, (short)(existing + 1));
                            return true;
                        }
                    case KuduType.Int32:
                        {
                            int existing = KuduEncoder.DecodeInt32(data);
                            if (existing == int.MaxValue)
                                return false;

                            KuduEncoder.EncodeInt32(data, existing + 1);
                            return true;
                        }
                    case KuduType.Int64:
                    case KuduType.UnixtimeMicros:
                        {
                            long existing = KuduEncoder.DecodeInt64(data);
                            if (existing == long.MaxValue)
                                return false;

                            KuduEncoder.EncodeInt64(data, existing + 1);
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
                    case KuduType.Decimal64:
                    case KuduType.Decimal128:
                        throw new NotImplementedException();
                    default:
                        throw new Exception($"Unsupported data type {type}");
                }
            }
            else
            {
                // Column is either string or binary.
                ReadOnlySpan<byte> data = GetVarLengthColumn(index);
                byte[] incremented = new byte[data.Length + 1];
                data.CopyTo(incremented);
                SetVarLengthData(index, incremented);
                return true;
            }
        }

        private void CheckColumn(int columnIndex, KuduType type)
        {
            ColumnSchema column = Schema.GetColumn(columnIndex);

            if (column.Type != type)
                throw new ArgumentException(
                    $"Can't set {column.Name} ({column.Type}) to {type}");
        }

        private void CheckFixedColumnSize(int columnIndex, KuduType type, int size)
        {
            ColumnSchema column = Schema.GetColumn(columnIndex);

            if (!column.IsFixedSize || column.Size != size)
                throw new ArgumentException(
                    $"Can't set {column.Name} ({column.Type}) to {type}");
        }

        private void CheckValue(int columnIndex)
        {
            if (!IsSet(columnIndex))
            {
                throw new ArgumentException("Column value is not set");
            }

            if (IsNull(columnIndex))
            {
                throw new ArgumentException("Column value is null");
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

        private int GetRowSize()
        {
            var size = _headerSize;

            for (int i = 0; i < Schema.Columns.Count; i++)
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

        private static byte[] CloneArray(byte[] array)
        {
            if (array == null)
                return null;

            var newArray = new byte[array.Length];
            array.CopyTo(newArray, 0);
            return newArray;
        }
    }
}
