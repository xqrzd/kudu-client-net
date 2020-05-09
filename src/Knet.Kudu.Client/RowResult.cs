using System;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public readonly ref struct RowResult
    {
        private readonly ResultSet _resultSet;
        private readonly ReadOnlySpan<byte> _rowData;
        private readonly ReadOnlySpan<byte> _indirectData;

        public RowResult(
            ResultSet resultSet,
            ReadOnlySpan<byte> rowData,
            ReadOnlySpan<byte> indirectData)
        {
            _resultSet = resultSet;
            _rowData = rowData;
            _indirectData = indirectData;
        }

        /// <summary>
        /// True if the RowResult has the IS_DELETED virtual column.
        /// </summary>
        public bool HasIsDeleted => _resultSet.Schema.HasIsDeleted;

        /// <summary>
        /// The value of the IS_DELETED virtual column.
        /// </summary>
        public bool IsDeleted => GetBool(_resultSet.Schema.IsDeletedIndex);

        public bool GetBool(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetBool(columnIndex);
        }

        public bool GetBool(int columnIndex)
        {
            CheckFixedTypeAndNotNull(columnIndex, 1);
            return ReadBool(columnIndex);
        }

        public bool? GetNullableBool(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableBool(columnIndex);
        }

        public bool? GetNullableBool(int columnIndex)
        {
            CheckFixedType(columnIndex, 1);

            if (IsNull(columnIndex))
                return null;

            return ReadBool(columnIndex);
        }

        private bool ReadBool(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 1);

            return KuduEncoder.DecodeBool(data);
        }

        public byte GetByte(string columnName) => (byte)GetSByte(columnName);

        public byte GetByte(int columnIndex) => (byte)GetSByte(columnIndex);

        public byte? GetNullableByte(string columnName) =>
            (byte?)GetNullableSByte(columnName);

        public byte? GetNullableByte(int columnIndex) =>
            (byte?)GetNullableSByte(columnIndex);

        public sbyte GetSByte(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetSByte(columnIndex);
        }

        public sbyte GetSByte(int columnIndex)
        {
            CheckFixedTypeAndNotNull(columnIndex, 1);
            return ReadSByte(columnIndex);
        }

        public sbyte? GetNullableSByte(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableSByte(columnIndex);
        }

        public sbyte? GetNullableSByte(int columnIndex)
        {
            CheckFixedType(columnIndex, 1);

            if (IsNull(columnIndex))
                return null;

            return ReadSByte(columnIndex);
        }

        private sbyte ReadSByte(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 1);

            return KuduEncoder.DecodeInt8(data);
        }

        public short GetInt16(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetInt16(columnIndex);
        }

        public short GetInt16(int columnIndex)
        {
            CheckFixedTypeAndNotNull(columnIndex, 2);
            return ReadInt16(columnIndex);
        }

        public short? GetNullableInt16(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableInt16(columnIndex);
        }

        public short? GetNullableInt16(int columnIndex)
        {
            CheckFixedType(columnIndex, 2);

            if (IsNull(columnIndex))
                return null;

            return ReadInt16(columnIndex);
        }

        private short ReadInt16(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 2);

            return KuduEncoder.DecodeInt16(data);
        }

        public int GetInt32(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetInt32(columnIndex);
        }

        public int GetInt32(int columnIndex)
        {
            CheckFixedTypeAndNotNull(columnIndex, 4);
            return ReadInt32(columnIndex);
        }

        public int? GetNullableInt32(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableInt32(columnIndex);
        }

        public int? GetNullableInt32(int columnIndex)
        {
            CheckFixedType(columnIndex, 4);

            if (IsNull(columnIndex))
                return null;

            return ReadInt32(columnIndex);
        }

        private int ReadInt32(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 4);

            return KuduEncoder.DecodeInt32(data);
        }

        public long GetInt64(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetInt64(columnIndex);
        }

        public long GetInt64(int columnIndex)
        {
            CheckFixedTypeAndNotNull(columnIndex, 8);
            return ReadInt64(columnIndex);
        }

        public long? GetNullableInt64(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableInt64(columnIndex);
        }

        public long? GetNullableInt64(int columnIndex)
        {
            CheckFixedType(columnIndex, 8);

            if (IsNull(columnIndex))
                return null;

            return ReadInt64(columnIndex);
        }

        private long ReadInt64(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 8);

            return KuduEncoder.DecodeInt64(data);
        }

        public DateTime GetDateTime(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetDateTime(columnIndex);
        }

        public DateTime GetDateTime(int columnIndex)
        {
            var columnSchema = _resultSet.GetColumnSchema(columnIndex);
            var type = columnSchema.Type;

            CheckNotNull(columnSchema, columnIndex);

            if (type == KuduType.UnixtimeMicros)
            {
                return ReadDateTime(columnIndex);
            }
            else if (type == KuduType.Date)
            {
                return ReadDate(columnIndex);
            }

            throw new Exception($"Column (name: {columnSchema.Name}," +
                $" index: {columnIndex}) is {type}; expected either" +
                $" {KuduType.UnixtimeMicros} or {KuduType.Date}");
        }

        public DateTime? GetNullableDateTime(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableDateTime(columnIndex);
        }

        public DateTime? GetNullableDateTime(int columnIndex)
        {
            var columnSchema = _resultSet.GetColumnSchema(columnIndex);
            var type = columnSchema.Type;

            if (IsNull(columnIndex))
                return null;

            if (type == KuduType.UnixtimeMicros)
            {
                return ReadDateTime(columnIndex);
            }
            else if (type == KuduType.Date)
            {
                return ReadDate(columnIndex);
            }

            throw new Exception($"Column (name: {columnSchema.Name}," +
                $" index: {columnIndex}) is {type}; expected either" +
                $" {KuduType.UnixtimeMicros} or {KuduType.Date}");
        }

        private DateTime ReadDateTime(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 8);

            return KuduEncoder.DecodeDateTime(data);
        }

        private DateTime ReadDate(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 4);

            return KuduEncoder.DecodeDate(data);
        }

        public float GetFloat(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetFloat(columnIndex);
        }

        public float GetFloat(int columnIndex)
        {
            CheckFixedTypeAndNotNull(columnIndex, 4);
            return ReadFloat(columnIndex);
        }

        public float? GetNullableFloat(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableFloat(columnIndex);
        }

        public float? GetNullableFloat(int columnIndex)
        {
            CheckFixedType(columnIndex, 4);

            if (IsNull(columnIndex))
                return null;

            return ReadFloat(columnIndex);
        }

        private float ReadFloat(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 4);

            return KuduEncoder.DecodeFloat(data);
        }

        public double GetDouble(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetDouble(columnIndex);
        }

        public double GetDouble(int columnIndex)
        {
            CheckFixedTypeAndNotNull(columnIndex, 8);
            return ReadDouble(columnIndex);
        }

        public double? GetNullableDouble(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableDouble(columnIndex);
        }

        public double? GetNullableDouble(int columnIndex)
        {
            CheckFixedType(columnIndex, 8);

            if (IsNull(columnIndex))
                return null;

            return ReadDouble(columnIndex);
        }

        private double ReadDouble(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 8);

            return KuduEncoder.DecodeDouble(data);
        }

        public decimal GetDecimal(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetDecimal(columnIndex);
        }

        public decimal GetDecimal(int columnIndex)
        {
            // TODO: Type check here.
            return ReadDecimal(columnIndex);
        }

        public decimal? GetNullableDecimal(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableDecimal(columnIndex);
        }

        public decimal? GetNullableDecimal(int columnIndex)
        {
            // TODO: Type check here.

            if (IsNull(columnIndex))
                return null;

            return ReadDecimal(columnIndex);
        }

        private decimal ReadDecimal(int columnIndex)
        {
            ColumnSchema column = _resultSet.GetColumnSchema(columnIndex);
            int scale = column.TypeAttributes.Scale;

            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, column.Size);

            return KuduEncoder.DecodeDecimal(data, column.Type, scale);
        }

        /// <summary>
        /// Get the raw value of a fixed length data column.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        public ReadOnlySpan<byte> GetRawFixed(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetRawFixed(columnIndex);
        }

        /// <summary>
        /// Get the raw value of a fixed length data column.
        /// </summary>
        /// <param name="columnIndex">The column index.</param>
        public ReadOnlySpan<byte> GetRawFixed(int columnIndex)
        {
            ColumnSchema column = _resultSet.GetColumnSchema(columnIndex);

            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, column.Size);

            return data;
        }

        public string GetString(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetString(columnIndex);
        }

        public string GetString(int columnIndex)
        {
            //checkNull(columnIndex);
            //checkType(columnIndex, Type.STRING);

            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> offsetData = _rowData.Slice(position, 8);
            ReadOnlySpan<byte> lengthData = _rowData.Slice(position + 8, 8);

            int offset = (int)KuduEncoder.DecodeInt64(offsetData);
            int length = (int)KuduEncoder.DecodeInt64(lengthData);

            ReadOnlySpan<byte> data = _indirectData.Slice(offset, length);

            return KuduEncoder.DecodeString(data);
        }

        public ReadOnlySpan<byte> GetBinary(int columnIndex)
        {
            CheckVariableLengthType(columnIndex);

            if (IsNull(columnIndex))
                return default;

            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> offsetData = _rowData.Slice(position, 8);
            ReadOnlySpan<byte> lengthData = _rowData.Slice(position + 8, 8);

            int offset = (int)KuduEncoder.DecodeInt64(offsetData);
            int length = (int)KuduEncoder.DecodeInt64(lengthData);

            return _indirectData.Slice(offset, length);
        }

        public bool IsNull(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return IsNull(columnIndex);
        }

        public bool IsNull(int columnIndex)
        {
            if (!_resultSet.HasNullableColumns)
                return false;

            int nullBitSetOffset = _resultSet.NullBitSetOffset;
            bool isNull = BitmapGet(nullBitSetOffset, columnIndex);
            return isNull;
        }

        private void CheckFixedType(int columnIndex, int size)
        {
            ColumnSchema columnSchema = _resultSet.GetColumnSchema(columnIndex);

            if (!columnSchema.IsFixedSize || columnSchema.Size != size)
            {
                throw new Exception($"Column (name: {columnSchema.Name}," +
                    $" index: {columnIndex}) is of size {columnSchema.Size} but was" +
                    $" requested as a size {size}");
            }
        }

        private void CheckNotNull(ColumnSchema columnSchema, int columnIndex)
        {
            if (IsNull(columnIndex))
            {
                throw new Exception($"The requested column (name: {columnSchema.Name}," +
                    $" index: {columnIndex}) is null");
            }
        }

        private void CheckFixedTypeAndNotNull(int columnIndex, int size)
        {
            ColumnSchema columnSchema = _resultSet.GetColumnSchema(columnIndex);

            CheckNotNull(columnSchema, columnIndex);

            if (!columnSchema.IsFixedSize || columnSchema.Size != size)
            {
                throw new Exception($"Column (name: {columnSchema.Name}," +
                    $" index: {columnIndex}) is of size {columnSchema.Size} but was" +
                    $" read as size {size}");
            }
        }

        private void CheckVariableLengthType(int columnIndex)
        {
            ColumnSchema columnSchema = _resultSet.GetColumnSchema(columnIndex);

            if (columnSchema.IsFixedSize)
            {
                throw new Exception($"Column (name: {columnSchema.Name}," +
                    $" index: {columnIndex}) is fixed size but was" +
                    $" read as variable length");
            }
        }

        private bool BitmapGet(int offset, int index)
        {
            return (_rowData[offset + (index / 8)] & (1 << (index % 8))) != 0;
        }
    }
}
