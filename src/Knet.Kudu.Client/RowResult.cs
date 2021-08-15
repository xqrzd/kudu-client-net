using System;
using System.Text;

namespace Knet.Kudu.Client
{
    public readonly struct RowResult
    {
        private readonly ResultSet _resultSet;
        private readonly int _index;

        internal RowResult(ResultSet resultSet, int index)
        {
            _resultSet = resultSet;
            _index = index;
        }

        /// <summary>
        /// True if the RowResult has the IS_DELETED virtual column.
        /// </summary>
        public bool HasIsDeleted => _resultSet.Schema.HasIsDeleted;

        /// <summary>
        /// The value of the IS_DELETED virtual column.
        /// </summary>
        public bool IsDeleted => GetBool(_resultSet.Schema.IsDeletedIndex);

        public bool GetBool(string columnName) =>
            _resultSet.GetBool(columnName, _index);

        public bool GetBool(int columnIndex) =>
            _resultSet.GetBool(columnIndex, _index);

        public bool? GetNullableBool(string columnName) =>
            _resultSet.GetNullableBool(columnName, _index);

        public bool? GetNullableBool(int columnIndex) =>
            _resultSet.GetNullableBool(columnIndex, _index);

        public byte GetByte(string columnName) =>
            _resultSet.GetByte(columnName, _index);

        public byte GetByte(int columnIndex) =>
            _resultSet.GetByte(columnIndex, _index);

        public byte? GetNullableByte(string columnName) =>
            _resultSet.GetNullableByte(columnName, _index);

        public byte? GetNullableByte(int columnIndex) =>
            _resultSet.GetNullableByte(columnIndex, _index);

        public sbyte GetSByte(string columnName) =>
            _resultSet.GetSByte(columnName, _index);

        public sbyte GetSByte(int columnIndex) =>
            _resultSet.GetSByte(columnIndex, _index);

        public sbyte? GetNullableSByte(string columnName) =>
            _resultSet.GetNullableSByte(columnName, _index);

        public sbyte? GetNullableSByte(int columnIndex) =>
            _resultSet.GetNullableSByte(columnIndex, _index);

        public short GetInt16(string columnName) =>
            _resultSet.GetInt16(columnName, _index);

        public short GetInt16(int columnIndex) =>
            _resultSet.GetInt16(columnIndex, _index);

        public short? GetNullableInt16(string columnName) =>
            _resultSet.GetNullableInt16(columnName, _index);

        public short? GetNullableInt16(int columnIndex) =>
            _resultSet.GetNullableInt16(columnIndex, _index);

        public int GetInt32(string columnName) =>
            _resultSet.GetInt32(columnName, _index);

        public int GetInt32(int columnIndex) =>
            _resultSet.GetInt32(columnIndex, _index);

        public int? GetNullableInt32(string columnName) =>
            _resultSet.GetNullableInt32(columnName, _index);

        public int? GetNullableInt32(int columnIndex) =>
            _resultSet.GetNullableInt32(columnIndex, _index);

        public long GetInt64(string columnName) =>
            _resultSet.GetInt64(columnName, _index);

        public long GetInt64(int columnIndex) =>
            _resultSet.GetInt64(columnIndex, _index);

        public long? GetNullableInt64(string columnName) =>
            _resultSet.GetNullableInt64(columnName, _index);

        public long? GetNullableInt64(int columnIndex) =>
            _resultSet.GetNullableInt64(columnIndex, _index);

        public DateTime GetDateTime(string columnName) =>
            _resultSet.GetDateTime(columnName, _index);

        public DateTime GetDateTime(int columnIndex) =>
            _resultSet.GetDateTime(columnIndex, _index);

        public DateTime? GetNullableDateTime(string columnName) =>
            _resultSet.GetNullableDateTime(columnName, _index);

        public DateTime? GetNullableDateTime(int columnIndex) =>
            _resultSet.GetNullableDateTime(columnIndex, _index);

        public float GetFloat(string columnName) =>
            _resultSet.GetFloat(columnName, _index);

        public float GetFloat(int columnIndex) =>
            _resultSet.GetFloat(columnIndex, _index);

        public float? GetNullableFloat(string columnName) =>
            _resultSet.GetNullableFloat(columnName, _index);

        public float? GetNullableFloat(int columnIndex) =>
            _resultSet.GetNullableFloat(columnIndex, _index);

        public double GetDouble(string columnName) =>
            _resultSet.GetDouble(columnName, _index);

        public double GetDouble(int columnIndex) =>
            _resultSet.GetDouble(columnIndex, _index);

        public double? GetNullableDouble(string columnName) =>
            _resultSet.GetNullableDouble(columnName, _index);

        public double? GetNullableDouble(int columnIndex) =>
            _resultSet.GetNullableDouble(columnIndex, _index);

        public decimal GetDecimal(string columnName) =>
            _resultSet.GetDecimal(columnName, _index);

        public decimal GetDecimal(int columnIndex) =>
            _resultSet.GetDecimal(columnIndex, _index);

        public decimal? GetNullableDecimal(string columnName) =>
            _resultSet.GetNullableDecimal(columnName, _index);

        public decimal? GetNullableDecimal(int columnIndex) =>
            _resultSet.GetNullableDecimal(columnIndex, _index);

        /// <summary>
        /// Get the raw value of a fixed length data column.
        /// </summary>
        /// <param name="columnName">The column name.</param>
        public ReadOnlySpan<byte> GetRawFixed(string columnName) =>
            _resultSet.GetRawFixed(columnName, _index);

        /// <summary>
        /// Get the raw value of a fixed length data column.
        /// </summary>
        /// <param name="columnIndex">The column index.</param>
        public ReadOnlySpan<byte> GetRawFixed(int columnIndex) =>
            _resultSet.GetRawFixed(columnIndex, _index);

        public string GetString(string columnName) =>
            _resultSet.GetString(columnName, _index);

        public string GetString(int columnIndex) =>
            _resultSet.GetString(columnIndex, _index);

        public string GetNullableString(string columnName) =>
            _resultSet.GetNullableString(columnName, _index);

        public string GetNullableString(int columnIndex) =>
            _resultSet.GetNullableString(columnIndex, _index);

        public ReadOnlySpan<byte> GetBinary(string columnName) =>
            _resultSet.GetBinary(columnName, _index);

        public ReadOnlySpan<byte> GetBinary(int columnIndex) =>
            _resultSet.GetBinary(columnIndex, _index);

        public bool IsNull(string columnName) =>
            _resultSet.IsNull(columnName, _index);

        public bool IsNull(int columnIndex) =>
            _resultSet.IsNull(columnIndex, _index);

        public override string ToString()
        {
            var stringBuilder = new StringBuilder();
            var columns = _resultSet.Schema.Columns;
            var numColumns = columns.Count;

            for (int i = 0; i < numColumns; i++)
            {
                var column = columns[i];
                var type = column.Type;

                if (i != 0)
                    stringBuilder.Append(", ");

                stringBuilder.Append($"{type.ToString().ToUpper()} {column.Name}");

                if (column.TypeAttributes != null)
                    stringBuilder.Append(column.TypeAttributes.ToStringForType(type));

                stringBuilder.Append('=');

                if (IsNull(i))
                {
                    stringBuilder.Append("NULL");
                }
                else
                {
                    switch (type)
                    {
                        case KuduType.Int8:
                            stringBuilder.Append(GetSByte(i));
                            break;
                        case KuduType.Int16:
                            stringBuilder.Append(GetInt16(i));
                            break;
                        case KuduType.Int32:
                            stringBuilder.Append(GetInt32(i));
                            break;
                        case KuduType.Int64:
                            stringBuilder.Append(GetInt64(i));
                            break;
                        case KuduType.Date:
                        case KuduType.UnixtimeMicros:
                            stringBuilder.Append(GetDateTime(i));
                            break;
                        case KuduType.String:
                        case KuduType.Varchar:
                            stringBuilder.Append(GetString(i));
                            break;
                        case KuduType.Binary:
                            stringBuilder.Append(BitConverter.ToString(GetBinary(i).ToArray()));
                            break;
                        case KuduType.Float:
                            stringBuilder.Append(GetFloat(i));
                            break;
                        case KuduType.Double:
                            stringBuilder.Append(GetDouble(i));
                            break;
                        case KuduType.Bool:
                            stringBuilder.Append(GetBool(i));
                            break;
                        case KuduType.Decimal32:
                        case KuduType.Decimal64:
                        case KuduType.Decimal128:
                            stringBuilder.Append(GetDecimal(i));
                            break;
                        default:
                            stringBuilder.Append("<unknown type!>");
                            break;
                    }
                }
            }

            return stringBuilder.ToString();
        }
    }
}
