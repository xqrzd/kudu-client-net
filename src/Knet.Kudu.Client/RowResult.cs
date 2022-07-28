using System;
using System.Text;

namespace Knet.Kudu.Client;

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

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Bool"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public bool GetBool(string columnName) =>
        _resultSet.GetBool(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Bool"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public bool GetBool(int columnIndex) =>
        _resultSet.GetBool(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Bool"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public bool? GetNullableBool(string columnName) =>
        _resultSet.GetNullableBool(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Bool"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public bool? GetNullableBool(int columnIndex) =>
        _resultSet.GetNullableBool(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public byte GetByte(string columnName) =>
        _resultSet.GetByte(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public byte GetByte(int columnIndex) =>
        _resultSet.GetByte(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public byte? GetNullableByte(string columnName) =>
        _resultSet.GetNullableByte(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public byte? GetNullableByte(int columnIndex) =>
        _resultSet.GetNullableByte(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public sbyte GetSByte(string columnName) =>
        _resultSet.GetSByte(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public sbyte GetSByte(int columnIndex) =>
        _resultSet.GetSByte(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public sbyte? GetNullableSByte(string columnName) =>
        _resultSet.GetNullableSByte(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int8"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public sbyte? GetNullableSByte(int columnIndex) =>
        _resultSet.GetNullableSByte(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int16"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public short GetInt16(string columnName) =>
        _resultSet.GetInt16(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int16"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public short GetInt16(int columnIndex) =>
        _resultSet.GetInt16(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int16"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public short? GetNullableInt16(string columnName) =>
        _resultSet.GetNullableInt16(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int16"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public short? GetNullableInt16(int columnIndex) =>
        _resultSet.GetNullableInt16(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int32"/> or
    /// <see cref="KuduType.Date"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public int GetInt32(string columnName) =>
        _resultSet.GetInt32(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int32"/> or
    /// <see cref="KuduType.Date"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public int GetInt32(int columnIndex) =>
        _resultSet.GetInt32(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int32"/> or
    /// <see cref="KuduType.Date"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public int? GetNullableInt32(string columnName) =>
        _resultSet.GetNullableInt32(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int32"/> or
    /// <see cref="KuduType.Date"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public int? GetNullableInt32(int columnIndex) =>
        _resultSet.GetNullableInt32(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int64"/> or
    /// <see cref="KuduType.UnixtimeMicros"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public long GetInt64(string columnName) =>
        _resultSet.GetInt64(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int64"/> or
    /// <see cref="KuduType.UnixtimeMicros"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public long GetInt64(int columnIndex) =>
        _resultSet.GetInt64(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int64"/> or
    /// <see cref="KuduType.UnixtimeMicros"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public long? GetNullableInt64(string columnName) =>
        _resultSet.GetNullableInt64(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Int64"/> or
    /// <see cref="KuduType.UnixtimeMicros"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public long? GetNullableInt64(int columnIndex) =>
        _resultSet.GetNullableInt64(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.UnixtimeMicros"/> or
    /// <see cref="KuduType.Date"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public DateTime GetDateTime(string columnName) =>
        _resultSet.GetDateTime(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.UnixtimeMicros"/> or
    /// <see cref="KuduType.Date"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public DateTime GetDateTime(int columnIndex) =>
        _resultSet.GetDateTime(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.UnixtimeMicros"/> or
    /// <see cref="KuduType.Date"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public DateTime? GetNullableDateTime(string columnName) =>
        _resultSet.GetNullableDateTime(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.UnixtimeMicros"/> or
    /// <see cref="KuduType.Date"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public DateTime? GetNullableDateTime(int columnIndex) =>
        _resultSet.GetNullableDateTime(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Float"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public float GetFloat(string columnName) =>
        _resultSet.GetFloat(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Float"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public float GetFloat(int columnIndex) =>
        _resultSet.GetFloat(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Float"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public float? GetNullableFloat(string columnName) =>
        _resultSet.GetNullableFloat(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Float"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public float? GetNullableFloat(int columnIndex) =>
        _resultSet.GetNullableFloat(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Double"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public double GetDouble(string columnName) =>
        _resultSet.GetDouble(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Double"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public double GetDouble(int columnIndex) =>
        _resultSet.GetDouble(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Double"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public double? GetNullableDouble(string columnName) =>
        _resultSet.GetNullableDouble(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Double"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public double? GetNullableDouble(int columnIndex) =>
        _resultSet.GetNullableDouble(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Decimal32"/> or
    /// <see cref="KuduType.Decimal64"/> or <see cref="KuduType.Decimal128"/>
    /// and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public decimal GetDecimal(string columnName) =>
        _resultSet.GetDecimal(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Decimal32"/> or
    /// <see cref="KuduType.Decimal64"/> or <see cref="KuduType.Decimal128"/>
    /// and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public decimal GetDecimal(int columnIndex) =>
        _resultSet.GetDecimal(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Decimal32"/> or
    /// <see cref="KuduType.Decimal64"/> or <see cref="KuduType.Decimal128"/>
    /// and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public decimal? GetNullableDecimal(string columnName) =>
        _resultSet.GetNullableDecimal(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Decimal32"/> or
    /// <see cref="KuduType.Decimal64"/> or <see cref="KuduType.Decimal128"/>
    /// and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public decimal? GetNullableDecimal(int columnIndex) =>
        _resultSet.GetNullableDecimal(columnIndex, _index);

    /// <summary>
    /// Get the raw value of the given column.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>A zero-copy span of the raw value.</returns>
    public ReadOnlySpan<byte> GetSpan(string columnName) =>
        _resultSet.GetSpan(columnName, _index);

    /// <summary>
    /// Get the raw value of the given column.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    /// <returns>A zero-copy span of the raw value.</returns>
    public ReadOnlySpan<byte> GetSpan(int columnIndex) =>
        _resultSet.GetSpan(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.String"/> or
    /// <see cref="KuduType.Varchar"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public string GetString(string columnName) =>
        _resultSet.GetString(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.String"/> or
    /// <see cref="KuduType.Varchar"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public string GetString(int columnIndex) =>
        _resultSet.GetString(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.String"/> or
    /// <see cref="KuduType.Varchar"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public string? GetNullableString(string columnName) =>
        _resultSet.GetNullableString(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.String"/> or
    /// <see cref="KuduType.Varchar"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public string? GetNullableString(int columnIndex) =>
        _resultSet.GetNullableString(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Binary"/> and be non-null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public byte[] GetBinary(string columnName) =>
        _resultSet.GetBinary(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Binary"/> and be non-null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public byte[] GetBinary(int columnIndex) =>
        _resultSet.GetBinary(columnIndex, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Binary"/> and can be null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public byte[]? GetNullableBinary(string columnName) =>
        _resultSet.GetNullableBinary(columnName, _index);

    /// <summary>
    /// Gets the value of the specified column.
    /// The Kudu type must be <see cref="KuduType.Binary"/> and can be null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
    public byte[]? GetNullableBinary(int columnIndex) =>
        _resultSet.GetNullableBinary(columnIndex, _index);

    /// <summary>
    /// Get if the specified column is null.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    public bool IsNull(string columnName) =>
        _resultSet.IsNull(columnName, _index);

    /// <summary>
    /// Get if the specified column is null.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index.</param>
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
                        stringBuilder.Append(BitConverter.ToString(GetBinary(i)));
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
