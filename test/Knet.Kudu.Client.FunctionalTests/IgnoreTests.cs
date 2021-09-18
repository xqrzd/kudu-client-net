using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class IgnoreTests : IAsyncLifetime
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
    public async Task TestInsertIgnoreAfterInsertHasNoRowError()
    {
        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestInsertIgnoreAfterInsertHasNoRowError));

        var table = await _client.CreateTableAsync(builder);

        var rows = new[]
        {
                ClientTestUtil.CreateBasicSchemaInsert(table, 1),
                ClientTestUtil.CreateBasicSchemaUpsert(table, 1, 1, false),
                ClientTestUtil.CreateBasicSchemaInsertIgnore(table, 1)
            };

        await _client.WriteAsync(rows);

        var rowStrings = await ClientTestUtil.ScanTableToStringsAsync(_client, table);
        var rowString = Assert.Single(rowStrings);
        Assert.Equal(
            "INT32 key=1, INT32 column1_i=1, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=True", rowString);
    }

    [SkippableFact]
    public async Task TestInsertAfterInsertIgnoreHasRowError()
    {
        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestInsertAfterInsertIgnoreHasRowError));

        var table = await _client.CreateTableAsync(builder);

        var rows = new[]
        {
                ClientTestUtil.CreateBasicSchemaInsertIgnore(table, 1),
                ClientTestUtil.CreateBasicSchemaInsert(table, 1)
            };

        var exception = await Assert.ThrowsAsync<KuduWriteException>(
            () => _client.WriteAsync(rows));

        var rowError = Assert.Single(exception.PerRowErrors);
        Assert.True(rowError.IsAlreadyPresent);

        var rowStrings = await ClientTestUtil.ScanTableToStringsAsync(_client, table);
        var rowString = Assert.Single(rowStrings);
        Assert.Equal(
            "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=True", rowString);
    }

    [SkippableFact]
    public async Task TestInsertIgnore()
    {
        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestInsertIgnore));

        var table = await _client.CreateTableAsync(builder);

        // Test insert ignore implements normal insert.
        var insert = ClientTestUtil.CreateBasicSchemaInsertIgnore(table, 1);
        await _client.WriteAsync(new[] { insert });

        var rowStrings = await ClientTestUtil.ScanTableToStringsAsync(_client, table);
        var rowString = Assert.Single(rowStrings);
        Assert.Equal(
            "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=True", rowString);

        // Test insert ignore does not return a row error.
        await _client.WriteAsync(new[] { insert });

        rowStrings = await ClientTestUtil.ScanTableToStringsAsync(_client, table);
        rowString = Assert.Single(rowStrings);
        Assert.Equal(
            "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=True", rowString);
    }

    [SkippableFact]
    public async Task TestUpdateIgnore()
    {
        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestUpdateIgnore));

        var table = await _client.CreateTableAsync(builder);

        // Test update ignore does not return a row error.
        var update1 = ClientTestUtil.CreateBasicSchemaUpdateIgnore(table, 1, 1, false);
        await _client.WriteAsync(new[] { update1 });
        Assert.Empty(await ClientTestUtil.ScanTableToStringsAsync(_client, table));

        var insert = ClientTestUtil.CreateBasicSchemaInsert(table, 1);
        await _client.WriteAsync(new[] { insert });
        Assert.Single(await ClientTestUtil.ScanTableToStringsAsync(_client, table));

        // Test update ignore implements normal update.
        var update2 = ClientTestUtil.CreateBasicSchemaUpdateIgnore(table, 1, 2, false);
        await _client.WriteAsync(new[] { update2 });

        var rowStrings = await ClientTestUtil.ScanTableToStringsAsync(_client, table);
        var rowString = Assert.Single(rowStrings);
        Assert.Equal(
            "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=True", rowString);
    }

    [SkippableFact]
    public async Task TestDeleteIgnore()
    {
        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestDeleteIgnore));

        var table = await _client.CreateTableAsync(builder);

        // Test delete ignore does not return a row error.
        var delete = ClientTestUtil.CreateBasicSchemaDeleteIgnore(table, 1);
        await _client.WriteAsync(new[] { delete });

        var insert = ClientTestUtil.CreateBasicSchemaInsert(table, 1);
        await _client.WriteAsync(new[] { insert });
        Assert.Single(await ClientTestUtil.ScanTableToStringsAsync(_client, table));

        // Test delete ignore implements normal delete.
        await _client.WriteAsync(new[] { delete });
        Assert.Empty(await ClientTestUtil.ScanTableToStringsAsync(_client, table));
    }
}
