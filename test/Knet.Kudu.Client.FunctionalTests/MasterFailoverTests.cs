using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class MasterFailoverTests
{
    [SkippableTheory]
    [InlineData(KillBefore.CreateClient, true)]
    [InlineData(KillBefore.CreateClient, false)]
    [InlineData(KillBefore.CreateTable, true)]
    [InlineData(KillBefore.CreateTable, false)]
    [InlineData(KillBefore.OpenTable, true)]
    [InlineData(KillBefore.OpenTable, false)]
    [InlineData(KillBefore.ScanTable, true)]
    [InlineData(KillBefore.ScanTable, false)]
    public async Task TestMasterFailover(KillBefore killBefore, bool restart)
    {
        await using var harness = await new MiniKuduClusterBuilder()
            .NumMasters(3)
            .NumTservers(3)
            .BuildHarnessAsync();

        if (killBefore == KillBefore.CreateClient)
            await DoActionAsync();

        await using var client = harness.CreateClientBuilder()
            .SetDefaultOperationTimeout(TimeSpan.FromMinutes(1))
            .Build();

        if (killBefore == KillBefore.CreateTable)
            await DoActionAsync();

        var tableName = $"TestMasterFailover-killBefore={killBefore}";
        var builder = new TableBuilder()
            .SetTableName(tableName)
            .SetNumReplicas(1)
            .AddColumn("column_x", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("column_y", KuduType.String)
            .AddHashPartitions(buckets: 4, seed: 777, "column_x");

        var table = await client.CreateTableAsync(builder);
        Assert.Equal(tableName, table.TableName);

        if (killBefore == KillBefore.OpenTable)
            await DoActionAsync();

        var table2 = await client.OpenTableAsync(tableName);

        if (killBefore == KillBefore.ScanTable)
            await DoActionAsync();

        var scanner = client.NewScanBuilder(table2)
            .Build();

        var numRows = await scanner.CountAsync();
        Assert.Equal(0, numRows); // We didn't write any rows.

        Task DoActionAsync()
        {
            if (restart)
                return harness.RestartLeaderMasterAsync();
            else
                return harness.KillLeaderMasterServerAsync();
        }
    }

    public enum KillBefore
    {
        CreateClient,
        CreateTable,
        OpenTable,
        ScanTable
    }
}
