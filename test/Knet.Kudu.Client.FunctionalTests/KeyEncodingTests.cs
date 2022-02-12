using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class KeyEncodingTests : IAsyncLifetime
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
    public async Task TestAllPrimaryKeyTypes()
    {
        var tableBuilder = new TableBuilder(nameof(TestAllPrimaryKeyTypes))
            .AddColumn("int8", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("int16", KuduType.Int16, opt => opt.Key(true))
            .AddColumn("int32", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("int64", KuduType.Int64, opt => opt.Key(true))
            .AddColumn("string", KuduType.String, opt => opt.Key(true))
            .AddColumn("binary", KuduType.Binary, opt => opt.Key(true))
            .AddColumn("timestamp", KuduType.UnixtimeMicros, opt => opt.Key(true))
            .AddColumn("decimal32", KuduType.Decimal32, opt => opt.Key(true)
                .DecimalAttributes(DecimalUtil.MaxDecimal32Precision, 0))
            .AddColumn("decimal64", KuduType.Decimal64, opt => opt.Key(true)
                .DecimalAttributes(DecimalUtil.MaxDecimal64Precision, 0))
            .AddColumn("decimal128", KuduType.Decimal128, opt => opt.Key(true)
                .DecimalAttributes(DecimalUtil.MaxDecimal128Precision, 0))
            .AddColumn("varchar", KuduType.Varchar, opt => opt.Key(true)
                .VarcharAttributes(10))
            .AddColumn("date", KuduType.Date, opt => opt.Key(true))
            .AddColumn("bool", KuduType.Bool)      // not primary key type
            .AddColumn("float", KuduType.Float)    // not primary key type
            .AddColumn("double", KuduType.Double); // not primary key type

        var table = await _client.CreateTableAsync(tableBuilder);

        var insert = table.NewInsert();
        insert.SetSByte("int8", 1);
        insert.SetInt16("int16", 2);
        insert.SetInt32("int32", 3);
        insert.SetInt64("int64", 4);
        insert.SetString("string", "foo");
        insert.SetBinary("binary", "bar".ToUtf8ByteArray());
        insert.SetInt64("timestamp", 6);
        insert.SetDecimal("decimal32", DecimalUtil.MaxUnscaledDecimal32);
        insert.SetDecimal("decimal64", DecimalUtil.MaxUnscaledDecimal64);
        insert.SetDecimal("decimal128", decimal.Truncate(decimal.MaxValue));
        insert.SetString("varchar", "varchar bar");
        insert.SetDateTime("date", EpochTime.FromUnixTimeDays(0));
        insert.SetBool("bool", true);
        insert.SetFloat("float", 7.8f);
        insert.SetDouble("double", 9.9);

        await _client.WriteAsync(new[] { insert });

        var scanner = _client.NewScanBuilder(table).Build();

        var scannedRows = 0;
        await foreach (var resultSet in scanner)
        {
            foreach (var row in resultSet)
            {
                scannedRows++;
                Assert.Equal(1, row.GetSByte("int8"));
                Assert.Equal(2, row.GetInt16("int16"));
                Assert.Equal(3, row.GetInt32("int32"));
                Assert.Equal(4, row.GetInt64("int64"));
                Assert.Equal("foo", row.GetString("string"));
                Assert.Equal("bar".ToUtf8ByteArray(), row.GetBinary("binary"));
                Assert.Equal(6, row.GetInt64("timestamp"));
                Assert.Equal(DecimalUtil.MaxUnscaledDecimal32, row.GetDecimal("decimal32"));
                Assert.Equal(DecimalUtil.MaxUnscaledDecimal64, row.GetDecimal("decimal64"));
                Assert.Equal(decimal.Truncate(decimal.MaxValue), row.GetDecimal("decimal128"));
                Assert.Equal("varchar ba", row.GetString("varchar"));
                Assert.Equal(0, row.GetInt32("date"));
                Assert.True(row.GetBool("bool"));
                Assert.Equal(7.8f, row.GetFloat("float"));
                Assert.Equal(9.9, row.GetDouble("double"));
            }
        }

        Assert.Equal(1, scannedRows);
    }
}
