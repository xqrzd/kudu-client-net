using System;
using System.Text;
using Knet.Kudu.Client.Internal;
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
            CheckTypeNotNull(columnIndex, KuduType.Bool);
            return ReadBool(columnIndex);
        }

        public bool? GetNullableBool(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableBool(columnIndex);
        }

        public bool? GetNullableBool(int columnIndex)
        {
            CheckType(columnIndex, KuduType.Bool);

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

        public byte GetByte(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetByte(columnIndex);
        }

        public byte GetByte(int columnIndex)
        {
            CheckTypeNotNull(columnIndex, KuduType.Int8);
            return ReadByte(columnIndex);
        }

        public byte? GetNullableByte(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableByte(columnIndex);
        }

        public byte? GetNullableByte(int columnIndex)
        {
            CheckType(columnIndex, KuduType.Int8);

            if (IsNull(columnIndex))
                return null;

            return ReadByte(columnIndex);
        }

        private byte ReadByte(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 1);

            return KuduEncoder.DecodeUInt8(data);
        }

        public sbyte GetSByte(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetSByte(columnIndex);
        }

        public sbyte GetSByte(int columnIndex)
        {
            CheckTypeNotNull(columnIndex, KuduType.Int8);
            return ReadSByte(columnIndex);
        }

        public sbyte? GetNullableSByte(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableSByte(columnIndex);
        }

        public sbyte? GetNullableSByte(int columnIndex)
        {
            CheckType(columnIndex, KuduType.Int8);

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
            CheckTypeNotNull(columnIndex, KuduType.Int16);
            return ReadInt16(columnIndex);
        }

        public short? GetNullableInt16(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableInt16(columnIndex);
        }

        public short? GetNullableInt16(int columnIndex)
        {
            CheckType(columnIndex, KuduType.Int16);

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
            CheckTypeNotNull(columnIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);
            return ReadInt32(columnIndex);
        }

        public int? GetNullableInt32(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableInt32(columnIndex);
        }

        public int? GetNullableInt32(int columnIndex)
        {
            CheckType(columnIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);

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
            CheckTypeNotNull(columnIndex,
                KuduTypeFlags.Int64 |
                KuduTypeFlags.UnixtimeMicros);

            return ReadInt64(columnIndex);
        }

        public long? GetNullableInt64(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableInt64(columnIndex);
        }

        public long? GetNullableInt64(int columnIndex)
        {
            CheckType(columnIndex,
                KuduTypeFlags.Int64 |
                KuduTypeFlags.UnixtimeMicros);

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

            return KuduTypeValidation.ThrowException<DateTime>(columnSchema,
                KuduTypeFlags.UnixtimeMicros |
                KuduTypeFlags.Date);
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

            return KuduTypeValidation.ThrowException<DateTime>(columnSchema,
                KuduTypeFlags.UnixtimeMicros |
                KuduTypeFlags.Date);
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
            CheckTypeNotNull(columnIndex, KuduType.Float);
            return ReadFloat(columnIndex);
        }

        public float? GetNullableFloat(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableFloat(columnIndex);
        }

        public float? GetNullableFloat(int columnIndex)
        {
            CheckType(columnIndex, KuduType.Float);

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
            CheckTypeNotNull(columnIndex, KuduType.Double);
            return ReadDouble(columnIndex);
        }

        public double? GetNullableDouble(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableDouble(columnIndex);
        }

        public double? GetNullableDouble(int columnIndex)
        {
            CheckType(columnIndex, KuduType.Double);

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
            CheckTypeNotNull(columnIndex,
                KuduTypeFlags.Decimal32 |
                KuduTypeFlags.Decimal64 |
                KuduTypeFlags.Decimal128);

            return ReadDecimal(columnIndex);
        }

        public decimal? GetNullableDecimal(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableDecimal(columnIndex);
        }

        public decimal? GetNullableDecimal(int columnIndex)
        {
            CheckType(columnIndex,
                KuduTypeFlags.Decimal32 |
                KuduTypeFlags.Decimal64 |
                KuduTypeFlags.Decimal128);

            if (IsNull(columnIndex))
                return null;

            return ReadDecimal(columnIndex);
        }

        private decimal ReadDecimal(int columnIndex)
        {
            ColumnSchema column = _resultSet.GetColumnSchema(columnIndex);
            int scale = column.TypeAttributes.Scale.GetValueOrDefault();

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
            CheckTypeNotNull(columnIndex,
                KuduTypeFlags.String |
                KuduTypeFlags.Varchar);

            return ReadString(columnIndex);
        }

        public string GetNullableString(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableString(columnIndex);
        }

        public string GetNullableString(int columnIndex)
        {
            CheckType(columnIndex,
                KuduTypeFlags.String |
                KuduTypeFlags.Varchar);

            if (IsNull(columnIndex))
                return null;

            return ReadString(columnIndex);
        }

        private string ReadString(int columnIndex)
        {
            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> offsetData = _rowData.Slice(position, 8);
            ReadOnlySpan<byte> lengthData = _rowData.Slice(position + 8, 8);

            int offset = (int)KuduEncoder.DecodeInt64(offsetData);
            int length = (int)KuduEncoder.DecodeInt64(lengthData);

            ReadOnlySpan<byte> data = _indirectData.Slice(offset, length);

            return KuduEncoder.DecodeString(data);
        }

        public ReadOnlySpan<byte> GetBinary(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetBinary(columnIndex);
        }

        public ReadOnlySpan<byte> GetBinary(int columnIndex)
        {
            CheckType(columnIndex, KuduType.Binary);

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
            bool isNull = _rowData.GetBit(nullBitSetOffset, columnIndex);
            return isNull;
        }

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

                stringBuilder.Append("=");

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
                        default:
                            stringBuilder.Append("<unknown type!>");
                            break;
                    }
                }
            }

            return stringBuilder.ToString();
        }

        private ColumnSchema CheckType(int columnIndex, KuduType type)
        {
            var columnSchema = _resultSet.GetColumnSchema(columnIndex);
            KuduTypeValidation.ValidateColumnType(columnSchema, type);
            return columnSchema;
        }

        private ColumnSchema CheckType(int columnIndex, KuduTypeFlags typeFlags)
        {
            var columnSchema = _resultSet.GetColumnSchema(columnIndex);
            KuduTypeValidation.ValidateColumnType(columnSchema, typeFlags);
            return columnSchema;
        }

        private ColumnSchema CheckTypeNotNull(int columnIndex, KuduType type)
        {
            var columnSchema = CheckType(columnIndex, type);
            CheckNotNull(columnSchema, columnIndex);
            return columnSchema;
        }

        private ColumnSchema CheckTypeNotNull(int columnIndex, KuduTypeFlags typeFlags)
        {
            var columnSchema = CheckType(columnIndex, typeFlags);
            CheckNotNull(columnSchema, columnIndex);
            return columnSchema;
        }

        private void CheckNotNull(ColumnSchema columnSchema, int columnIndex)
        {
            if (IsNull(columnIndex))
            {
                KuduTypeValidation.ThrowNullException(columnSchema);
            }
        }
    }
}
