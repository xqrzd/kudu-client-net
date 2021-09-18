using System.Collections.Generic;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests;

public class PartialRowTests
{
    [Fact]
    public void SetMin()
    {
        PartialRow row = GetPartialRowWithAllTypes();

        for (int i = 0; i < row.Schema.Columns.Count; i++)
            row.SetMin(i);

        Assert.False(row.GetBool("bool"));
        Assert.Equal(sbyte.MinValue, row.GetSByte("int8"));
        Assert.Equal(short.MinValue, row.GetInt16("int16"));
        Assert.Equal(int.MinValue, row.GetInt32("int32"));
        Assert.Equal(long.MinValue, row.GetInt64("int64"));
        Assert.Equal(long.MinValue, row.GetInt64("timestamp"));
        Assert.Equal(EpochTime.FromUnixTimeDays(EpochTime.MinDateValue), row.GetDateTime("date"));
        Assert.Equal(float.MinValue, row.GetFloat("float"));
        Assert.Equal(double.MinValue, row.GetDouble("double"));
        Assert.Equal("", row.GetString("string"));
        Assert.Equal("", row.GetString("varchar"));
        Assert.Equal(new byte[0], row.GetBinary("binary"));
        Assert.Equal(-99.999m, row.GetDecimal("decimal32"));
        Assert.Equal(-99.999m, row.GetDecimal("decimal64"));
        Assert.Equal(-99.999m, row.GetDecimal("decimal128"));
    }

    [Fact]
    public void IncrementColumn()
    {
        PartialRow row = GetPartialRowWithAllTypes();

        // Boolean
        int boolIndex = row.Schema.GetColumnIndex("bool");
        row.SetBool(boolIndex, false);
        Assert.True(row.IncrementColumn(boolIndex));
        Assert.True(row.GetBool(boolIndex));
        Assert.False(row.IncrementColumn(boolIndex));

        // Int8
        int int8Index = row.Schema.GetColumnIndex("int8");
        row.SetSByte(int8Index, sbyte.MaxValue - 1);
        Assert.True(row.IncrementColumn(int8Index));
        Assert.Equal(sbyte.MaxValue, row.GetSByte(int8Index));
        Assert.False(row.IncrementColumn(int8Index));

        // Int16
        int int16Index = row.Schema.GetColumnIndex("int16");
        row.SetInt16(int16Index, short.MaxValue - 1);
        Assert.True(row.IncrementColumn(int16Index));
        Assert.Equal(short.MaxValue, row.GetInt16(int16Index));
        Assert.False(row.IncrementColumn(int16Index));

        // Int32
        int int32Index = row.Schema.GetColumnIndex("int32");
        row.SetInt32(int32Index, int.MaxValue - 1);
        Assert.True(row.IncrementColumn(int32Index));
        Assert.Equal(int.MaxValue, row.GetInt32(int32Index));
        Assert.False(row.IncrementColumn(int32Index));

        // Int64
        int int64Index = row.Schema.GetColumnIndex("int64");
        row.SetInt64(int64Index, long.MaxValue - 1);
        Assert.True(row.IncrementColumn(int64Index));
        Assert.Equal(long.MaxValue, row.GetInt64(int64Index));
        Assert.False(row.IncrementColumn(int64Index));

        // Date
        int dateIndex = row.Schema.GetColumnIndex("date");
        row.SetDateTime(dateIndex, EpochTime.FromUnixTimeDays(EpochTime.MaxDateValue - 1));
        Assert.True(row.IncrementColumn(dateIndex));
        Assert.Equal(EpochTime.FromUnixTimeDays(EpochTime.MaxDateValue), row.GetDateTime(dateIndex));
        Assert.False(row.IncrementColumn(dateIndex));

        // Float
        int floatIndex = row.Schema.GetColumnIndex("float");
        row.SetFloat(floatIndex, float.MaxValue);
        Assert.True(row.IncrementColumn(floatIndex));
        Assert.Equal(float.PositiveInfinity, row.GetFloat(floatIndex));
        Assert.False(row.IncrementColumn(floatIndex));

        // Double
        int doubleIndex = row.Schema.GetColumnIndex("double");
        row.SetDouble(doubleIndex, double.MaxValue);
        Assert.True(row.IncrementColumn(doubleIndex));
        Assert.Equal(double.PositiveInfinity, row.GetDouble(doubleIndex));
        Assert.False(row.IncrementColumn(doubleIndex));

        // Decimal32
        int decimalIndex32 = row.Schema.GetColumnIndex("decimal32");
        // Decimal with precision 5, scale 3 has a max of 99.999
        row.SetDecimal(decimalIndex32, 99.998m);
        Assert.True(row.IncrementColumn(decimalIndex32));
        Assert.Equal(99.999m, row.GetDecimal(decimalIndex32));
        Assert.False(row.IncrementColumn(decimalIndex32));

        // Decimal64
        int decimalIndex64 = row.Schema.GetColumnIndex("decimal64");
        // Decimal with precision 5, scale 3 has a max of 99.999
        row.SetDecimal(decimalIndex64, 99.998m);
        Assert.True(row.IncrementColumn(decimalIndex64));
        Assert.Equal(99.999m, row.GetDecimal(decimalIndex64));
        Assert.False(row.IncrementColumn(decimalIndex64));

        // Decimal128
        int decimalIndex128 = row.Schema.GetColumnIndex("decimal128");
        // Decimal with precision 5, scale 3 has a max of 99.999
        row.SetDecimal(decimalIndex128, 99.998m);
        Assert.True(row.IncrementColumn(decimalIndex128));
        Assert.Equal(99.999m, row.GetDecimal(decimalIndex128));
        Assert.False(row.IncrementColumn(decimalIndex128));

        // String
        int stringIndex = row.Schema.GetColumnIndex("string");
        row.SetString(stringIndex, "hello");
        Assert.True(row.IncrementColumn(stringIndex));
        Assert.Equal("hello\0", row.GetString(stringIndex));

        // Binary
        int binaryIndex = row.Schema.GetColumnIndex("binary");
        row.SetBinary(binaryIndex, new byte[] { 0, 1, 2, 3, 4 });
        Assert.True(row.IncrementColumn(binaryIndex));
        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 0 }, row.GetBinary(binaryIndex));

        // Varchar
        int varcharIndex = row.Schema.GetColumnIndex("varchar");
        row.SetString(varcharIndex, "hello");
        Assert.True(row.IncrementColumn(varcharIndex));
        Assert.Equal("hello\0", row.GetString(varcharIndex));
    }

    private PartialRow GetPartialRowWithAllTypes(bool isNullable = false)
    {
        var schema = new KuduSchema(new List<ColumnSchema>
            {
                new ColumnSchema("int8", KuduType.Int8, false, isNullable),
                new ColumnSchema("int16", KuduType.Int16, false, isNullable),
                new ColumnSchema("int32", KuduType.Int32, false, isNullable),
                new ColumnSchema("int64", KuduType.Int64, false, isNullable),
                new ColumnSchema("string", KuduType.String, false, isNullable),
                new ColumnSchema("bool", KuduType.Bool, false, isNullable),
                new ColumnSchema("float", KuduType.Float, false, isNullable),
                new ColumnSchema("double", KuduType.Double, false, isNullable),
                new ColumnSchema("binary", KuduType.Binary, false, isNullable),
                new ColumnSchema("timestamp", KuduType.UnixtimeMicros, false, isNullable),
                new ColumnSchema("date", KuduType.Date, false, isNullable),
                new ColumnSchema("decimal32", KuduType.Decimal32, false, isNullable,
                    typeAttributes: new ColumnTypeAttributes(5, 3, null)),
                new ColumnSchema("decimal64", KuduType.Decimal64, false, isNullable,
                    typeAttributes: new ColumnTypeAttributes(5, 3, null)),
                new ColumnSchema("decimal128", KuduType.Decimal128, false, isNullable,
                    typeAttributes: new ColumnTypeAttributes(5, 3, null)),
                new ColumnSchema("varchar", KuduType.Varchar, false, isNullable,
                    typeAttributes: new ColumnTypeAttributes(null, null, 10))
            });

        return new PartialRow(schema);
    }
}
