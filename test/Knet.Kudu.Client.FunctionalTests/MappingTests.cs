using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class MappingTests : IAsyncLifetime
{
    private KuduTestHarness _harness;
    private KuduClient _client;

    public async Task InitializeAsync()
    {
        _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
        _client = _harness.CreateClient();
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _harness.DisposeAsync();
    }

    [SkippableFact]
    public async Task TestConstructorWithProperties()
    {
        var builder = new TableBuilder(nameof(TestConstructorWithProperties))
            .AddColumn("Key", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("Column1", KuduType.String, opt => opt.Nullable(false))
            .AddColumn("Column2", KuduType.Int32, opt => opt.Nullable(false))
            .AddColumn("Column3", KuduType.String, opt => opt.Nullable(false));

        var values = new[]
        {
            new MixedRecord(1, "val-1") { Column2 = 100, Column3 = "val-2" },
            new MixedRecord(2, "val-3") { Column2 = 200, Column3 = "val-4" }
        };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetInt32("Key", value.Key);
            insert.SetString("Column1", value.Column1);
            insert.SetInt32("Column2", value.Column2);
            insert.SetString("Column3", value.Column3);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var rows = await ScanAsync<MixedRecord>(table);

        Assert.Equal(values, rows);
    }

    [SkippableFact]
    public async Task TestCasting()
    {
        var builder = new TableBuilder(nameof(TestCasting))
            .AddColumn("key", KuduType.String, opt => opt.Key(true))
            .AddColumn("column1", KuduType.Date)
            .AddColumn("column2", KuduType.Int8, opt => opt.Nullable(false))
            .AddColumn("column3", KuduType.Binary)
            .AddColumn("column4", KuduType.Int32, opt => opt.Nullable(false))
            .AddColumn("column5", KuduType.Int16);

        var table = await _client.CreateTableAsync(builder);

        var insert1 = table.NewInsert();
        var date = DateTime.Parse("5/5/2015");
        insert1.SetString("key", "Key-1");
        insert1.SetDateTime("column1", date);
        insert1.SetSByte("column2", 100);
        insert1.SetBinary("column3", new byte[] { 5, 6, 7 });
        insert1.SetInt32("column4", (int)TestEnum.Value1);
        insert1.SetInt16("column5", (short)TestEnum.Value2);

        var insert2 = table.NewInsert();
        insert2.SetString("key", "Key-2");
        insert2.SetSByte("column2", -100);
        insert2.SetInt32("column4", (int)TestEnum.Value0);

        await _client.WriteAsync(new[] { insert1, insert2 });

        var rows = await ScanAsync<CastingRecord>(table);

        Assert.Collection(rows, row =>
        {
            Assert.Equal("Key-1", row.Key);
            Assert.Equal(EpochTime.ToUnixTimeDays(date), row.Column1);
            Assert.Equal(100, row.Column2.Value);
            Assert.Equal(new byte[] { 5, 6, 7 }, row.Column3.ToArray());
            Assert.Equal(TestEnum.Value1, row.Column4);
            Assert.Equal(TestEnum.Value2, row.Column5);
        }, row =>
        {
            Assert.Equal("Key-2", row.Key);
            Assert.Null(row.Column1);
            Assert.Equal(-100, row.Column2.Value);
            Assert.Empty(row.Column3.ToArray());
            Assert.Equal(TestEnum.Value0, row.Column4);
            Assert.Null(row.Column5);
        });
    }

    [SkippableFact]
    public async Task TestMappingEveryType()
    {
        var builder = new TableBuilder(nameof(TestMappingEveryType))
            .AddColumn("Int32", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("Int32_N", KuduType.Int32, opt => opt.Nullable(true))
            .AddColumn("Bool", KuduType.Bool, opt => opt.Nullable(false))
            .AddColumn("Bool_N", KuduType.Bool, opt => opt.Nullable(true))
            .AddColumn("Int8", KuduType.Int8, opt => opt.Nullable(false))
            .AddColumn("Int8_N", KuduType.Int8, opt => opt.Nullable(true))
            .AddColumn("Int16", KuduType.Int16, opt => opt.Nullable(false))
            .AddColumn("Int16_N", KuduType.Int16, opt => opt.Nullable(true))
            .AddColumn("Int64", KuduType.Int64, opt => opt.Nullable(false))
            .AddColumn("Int64_N", KuduType.Int64, opt => opt.Nullable(true))
            .AddColumn("Float", KuduType.Float, opt => opt.Nullable(false))
            .AddColumn("Float_N", KuduType.Float, opt => opt.Nullable(true))
            .AddColumn("Double", KuduType.Double, opt => opt.Nullable(false))
            .AddColumn("Double_N", KuduType.Double, opt => opt.Nullable(true))
            .AddColumn("Decimal", KuduType.Decimal32, opt => opt.Nullable(false).DecimalAttributes(5, 3))
            .AddColumn("Decimal_N", KuduType.Decimal32, opt => opt.Nullable(true).DecimalAttributes(5, 3))
            .AddColumn("UnixTime", KuduType.UnixtimeMicros, opt => opt.Nullable(false))
            .AddColumn("UnixTime_N", KuduType.UnixtimeMicros, opt => opt.Nullable(true))
            .AddColumn("Date", KuduType.Date, opt => opt.Nullable(false))
            .AddColumn("Date_N", KuduType.Date, opt => opt.Nullable(true))
            .AddColumn("String", KuduType.String, opt => opt.Nullable(false))
            .AddColumn("String_N", KuduType.String, opt => opt.Nullable(true))
            .AddColumn("Varchar", KuduType.Varchar, opt => opt.Nullable(false).VarcharAttributes(10))
            .AddColumn("Varchar_N", KuduType.Varchar, opt => opt.Nullable(true).VarcharAttributes(10))
            .AddColumn("Binary", KuduType.Binary, opt => opt.Nullable(false))
            .AddColumn("Binary_N", KuduType.Binary, opt => opt.Nullable(true));

        var values = new[]
        {
            new AllTypesClass
            {
                Bool = false,
                Bool_N = true,
                Int8 = 12,
                Int8_N = 35,
                Int16 = 99,
                Int16_N = -3,
                Int32 = 1,
                Int32_N = 98473,
                Int64 = 435,
                Int64_N = -348,
                Float = 234.45f,
                Float_N = 123f,
                Double = 46546.565d,
                Double_N = -987.123d,
                Decimal = 12.123m,
                Decimal_N = 99.1m,
                UnixTime = DateTime.Parse("1/15/2022 9:42 PM").ToUniversalTime(),
                UnixTime_N = DateTime.Parse("2/17/2022 11:42:11 AM").ToUniversalTime(),
                Date = DateTime.UtcNow.Date,
                Date_N = DateTime.UtcNow.Date.AddDays(3),
                String = "string",
                String_N = "string_n",
                Varchar = "varchar",
                Varchar_N = "varchar_n",
                Binary = new byte[] { 1 },
                Binary_N = new byte[] { 2 }
            },
            new AllTypesClass
            {
                Bool = true,
                Int8 = 123,
                Int16 = 999,
                Int32 = 11,
                Int64 = 4356,
                Float = 235.45f,
                Double = 1111.565d,
                Decimal = 88.123m,
                UnixTime = DateTime.Parse("3/19/2021 5:00 PM").ToUniversalTime(),
                Date = DateTime.UtcNow.Date,
                String = "string-2",
                Varchar = "varchar-2",
                Binary = new byte[] { 3 }
            }
        };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetBool("Bool", value.Bool);
            insert.SetSByte("Int8", value.Int8);
            insert.SetInt16("Int16", value.Int16);
            insert.SetInt32("Int32", value.Int32);
            insert.SetInt64("Int64", value.Int64);
            insert.SetFloat("Float", value.Float);
            insert.SetDouble("Double", value.Double);
            insert.SetDecimal("Decimal", value.Decimal);
            insert.SetDateTime("UnixTime", value.UnixTime);
            insert.SetDateTime("Date", value.Date);
            insert.SetString("String", value.String);
            insert.SetString("Varchar", value.Varchar);
            insert.SetBinary("Binary", value.Binary);
            if (value.Bool_N.HasValue) insert.SetBool("Bool_N", value.Bool_N.Value);
            if (value.Int8_N.HasValue) insert.SetSByte("Int8_N", value.Int8_N.Value);
            if (value.Int16_N.HasValue) insert.SetInt16("Int16_N", value.Int16_N.Value);
            if (value.Int32_N.HasValue) insert.SetInt32("Int32_N", value.Int32_N.Value);
            if (value.Int64_N.HasValue) insert.SetInt64("Int64_N", value.Int64_N.Value);
            if (value.Float_N.HasValue) insert.SetFloat("Float_N", value.Float_N.Value);
            if (value.Double_N.HasValue) insert.SetDouble("Double_N", value.Double_N.Value);
            if (value.Decimal_N.HasValue) insert.SetDecimal("Decimal_N", value.Decimal_N.Value);
            if (value.UnixTime_N.HasValue) insert.SetDateTime("UnixTime_N", value.UnixTime_N.Value);
            if (value.Date_N.HasValue) insert.SetDateTime("Date_N", value.Date_N.Value);
            if (value.String_N is not null) insert.SetString("String_N", value.String_N);
            if (value.Varchar_N is not null) insert.SetString("Varchar_N", value.Varchar_N);
            if (value.Binary_N is not null) insert.SetBinary("Binary_N", value.Binary_N);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var rows = await ScanAsync<AllTypesClass>(table);

        Assert.Equal(values, rows);
    }

    [SkippableFact]
    public async Task TestScalar()
    {
        var builder = new TableBuilder(nameof(TestValueTuple))
            .AddColumn("key", KuduType.Int32, opt => opt.Key(true));

        var values = new[] { 0, 1, 2, 3, 4, 5 };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetInt32("key", value);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var rows = await ScanAsync<int>(table);

        Assert.Equal(values, rows);
    }

    [SkippableFact]
    public async Task TestDifferentProjection()
    {
        var builder = new TableBuilder(nameof(TestValueTuple))
            .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("extra_column", KuduType.Int32, opt => opt.Nullable(false));

        var values = new[] { 0, 1, 2, 3, 4, 5 };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetInt32("key", value);
            insert.SetInt32("extra_column", value * 2);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var scanner = _client.NewScanBuilder(table)
            .SetProjectedColumns("key")
            .SetReadMode(ReadMode.ReadYourWrites)
            .Build();

        var rows = await scanner.ScanToListAsync<int>();

        Assert.Equal(values, rows);
    }

    [SkippableFact]
    public async Task TestValueTuple()
    {
        var builder = new TableBuilder(nameof(TestValueTuple))
            .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("column_1", KuduType.Double, opt => opt.Nullable(false))
            .AddColumn("column_2", KuduType.Bool, opt => opt.Nullable(false))
            .AddColumn("column_3", KuduType.Bool, opt => opt.Nullable(false))
            .AddColumn("column_4", KuduType.String);

        var values = new[]
        {
            (0, 1.2d, true, true, "a"),
            (1, 2.2d, true, false, "b"),
            (2, 3.2d, false, true, null),
            (3, 4.2d, false, false, "c")
        };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetInt32("key", value.Item1);
            insert.SetDouble("column_1", value.Item2);
            insert.SetBool("column_2", value.Item3);
            insert.SetBool("column_3", value.Item4);
            if (value.Item5 is not null)
                insert.SetString("column_4", value.Item5);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var rows = await ScanAsync<(int, double, bool, bool, string)>(table);

        Assert.Equal(values, rows);
    }

    [SkippableFact]
    public async Task TestValueTupleProjectTooManyProperties()
    {
        var builder = new TableBuilder(nameof(TestValueTupleProjectTooManyProperties))
            .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("column_1", KuduType.Binary);

        var values = new[] { (0, new byte[] { 1, 2, 3 }) };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetInt32("key", value.Item1);
            insert.SetBinary("column_1", value.Item2);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => ScanAsync<(int, byte[], string)>(table));

        Assert.Equal(
            "ValueTuple has 3 properties, but projection schema has 2 columns",
            exception.Message);
    }

    [SkippableFact]
    public async Task TestValueTupleProjectTooFewProperties()
    {
        var builder = new TableBuilder(nameof(TestValueTupleProjectTooFewProperties))
            .AddColumn("key", KuduType.String, opt => opt.Key(true))
            .AddColumn("column_1", KuduType.String)
            .AddColumn("column_2", KuduType.String);

        var values = new[] { ("a", "b", "c") };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetString("key", value.Item1);
            insert.SetString("column_1", value.Item2);
            insert.SetString("column_2", value.Item2);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => ScanAsync<(string, string)>(table));

        Assert.Equal(
            "ValueTuple has 2 properties, but projection schema has 3 columns",
            exception.Message);
    }

    [SkippableFact]
    public async Task TestValueTupleProjectIncompatibleProperties()
    {
        var builder = new TableBuilder(nameof(TestValueTupleProjectIncompatibleProperties))
            .AddColumn("key", KuduType.String, opt => opt.Key(true))
            .AddColumn("column_1", KuduType.String);

        var values = new[] { ("a", "b") };

        var table = await _client.CreateTableAsync(builder);

        var rowsToInsert = values.Select(value =>
        {
            var insert = table.NewInsert();
            insert.SetString("key", value.Item1);
            insert.SetString("column_1", value.Item2);
            return insert;
        });

        await _client.WriteAsync(rowsToInsert);

        var exception = await Assert.ThrowsAsync<ArgumentException>(
            () => ScanAsync<(string, decimal)>(table));

        Assert.Equal(
            "Column `column_1 String?` cannot be converted to ValueTuple property `System.Decimal item2`",
            exception.Message);
    }

    private Task<List<T>> ScanAsync<T>(KuduTable table)
    {
        var scanner = _client.NewScanBuilder(table)
            .SetReadMode(ReadMode.ReadYourWrites)
            .Build();

        return scanner.ScanToListAsync<T>().AsTask();
    }

    private enum TestEnum
    {
        Value0 = 0,
        Value1 = 1,
        Value2 = 2
    }

    private record MixedRecord(int Key, string Column1)
    {
        public int Column2 { get; init; }
        public string Column3 { get; init; }
    }

    private record CastingRecord(
        string Key,
        long? Column1,
        short? Column2,
        ReadOnlyMemory<byte> Column3,
        TestEnum Column4,
        TestEnum? Column5);

    private class AllTypesClass : IEquatable<AllTypesClass>
    {
        public bool Bool { get; init; }
        public bool? Bool_N { get; init; }
        public sbyte Int8 { get; init; }
        public sbyte? Int8_N { get; init; }
        public short Int16 { get; init; }
        public short? Int16_N { get; init; }
        public int Int32 { get; init; }
        public int? Int32_N { get; init; }
        public long Int64 { get; init; }
        public long? Int64_N { get; init; }
        public float Float { get; init; }
        public float? Float_N { get; init; }
        public double Double { get; init; }
        public double? Double_N { get; init; }
        public decimal Decimal { get; init; }
        public decimal? Decimal_N { get; init; }
        public DateTime UnixTime { get; init; }
        public DateTime? UnixTime_N { get; init; }
        public DateTime Date { get; init; }
        public DateTime? Date_N { get; init; }
        public string String { get; init; }
        public string String_N { get; init; }
        public string Varchar { get; init; }
        public string Varchar_N { get; init; }
        public byte[] Binary { get; init; }
        public byte[] Binary_N { get; init; }

        public bool Equals(AllTypesClass other)
        {
            if (other is null)
                return false;

            Assert.Equal(Bool, other.Bool);
            Assert.Equal(Bool_N, other.Bool_N);
            Assert.Equal(Int8, other.Int8);
            Assert.Equal(Int8_N, other.Int8_N);
            Assert.Equal(Int16, other.Int16);
            Assert.Equal(Int16_N, other.Int16_N);
            Assert.Equal(Int32, other.Int32);
            Assert.Equal(Int32_N, other.Int32_N);
            Assert.Equal(Int64, other.Int64);
            Assert.Equal(Int64_N, other.Int64_N);
            Assert.Equal(Float, other.Float);
            Assert.Equal(Float_N, other.Float_N);
            Assert.Equal(Double, other.Double);
            Assert.Equal(Double_N, other.Double_N);
            Assert.Equal(Decimal, other.Decimal);
            Assert.Equal(Decimal_N, other.Decimal_N);
            Assert.Equal(UnixTime, other.UnixTime);
            Assert.Equal(UnixTime_N, other.UnixTime_N);
            Assert.Equal(Date, other.Date);
            Assert.Equal(Date_N, other.Date_N);
            Assert.Equal(String, other.String);
            Assert.Equal(String_N, other.String_N);
            Assert.Equal(Varchar, other.Varchar);
            Assert.Equal(Varchar_N, other.Varchar_N);
            Assert.Equal(Binary, other.Binary);
            Assert.Equal(Binary_N, other.Binary_N);

            return true;
        }

        public override bool Equals(object obj) => obj is AllTypesClass c && Equals(c);

        public override int GetHashCode() => Int32;
    }
}
