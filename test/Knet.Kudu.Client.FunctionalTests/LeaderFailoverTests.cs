using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class LeaderFailoverTests
{
    /// <summary>
    /// This test writes 3 rows, kills the leader, then tries to write another 3 rows.
    /// Finally it counts to make sure we have 6 of them.
    /// </summary>
    [SkippableTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TestFailover(bool restart)
    {
        await using var harness = await new MiniKuduClusterBuilder()
            .NumMasters(3)
            .NumTservers(3)
            .BuildHarnessAsync();

        await using var client = harness.CreateClient();

        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName("LeaderFailoverTest")
            .CreateBasicRangePartition();
        var table = await client.CreateTableAsync(builder);

        var rows = Enumerable.Range(0, 3)
            .Select(i => ClientTestUtil.CreateBasicSchemaInsert(table, i));

        await client.WriteAsync(rows);

        // Make sure the rows are in there before messing things up.
        long numRows = 0;
        var scanner = client.NewScanBuilder(table)
            .Build();

        await foreach (var resultSet in scanner)
        {
            numRows += resultSet.Count;
        }

        Assert.Equal(3, numRows);

        if (restart)
            await harness.RestartLeaderMasterAsync();
        else
            await harness.KillLeaderMasterServerAsync();

        var rows2 = Enumerable.Range(3, 3)
            .Select(i => ClientTestUtil.CreateBasicSchemaInsert(table, i));

        await client.WriteAsync(rows2);

        long numRows2 = 0;

        await foreach (var resultSet in scanner)
        {
            numRows2 += resultSet.Count;
        }

        Assert.Equal(6, numRows2);
    }
}
