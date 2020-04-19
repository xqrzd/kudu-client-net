using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class TableTests
    {
        private readonly string _tableName = "TestKuduTable";

        [SkippableFact]
        public async Task TestDimensionLabel()
        {
            await using var harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
            await using var client = harness.CreateClient();

            // Create a table with dimension label.
            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .CreateBasicNonCoveredRangePartitions()
                .SetDimensionLabel("labelA");

            var table = await client.CreateTableAsync(builder);

            // Add a range partition to the table with dimension label.
            await client.AlterTableAsync(new AlterTableBuilder(table)
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 300);
                    upper.SetInt32("key", 400);
                }, "labelB", RangePartitionBound.Inclusive, RangePartitionBound.Exclusive));

            var tablets = await client.GetTableLocationsAsync(table.TableId, null, 100);

            var dimensionMap = tablets
                .SelectMany(t => t.Replicas)
                .GroupBy(r => r.DimensionLabel)
                .OrderBy(g => g.Key);

            Assert.Collection(dimensionMap, d =>
            {
                Assert.Equal("labelA", d.Key);
                Assert.Equal(9, d.Count());
            }, d =>
            {
                Assert.Equal("labelB", d.Key);
                Assert.Equal(3, d.Count());
            });
        }

        [SkippableFact]
        public async Task TestGetTableStatistics()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddTabletServerFlag("--update_tablet_stats_interval_ms=200")
                .AddTabletServerFlag("--heartbeat_interval_ms=100")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            // Create a table.
            var builder = ClientTestUtil.GetBasicSchema().SetTableName(_tableName);
            var table = await client.CreateTableAsync(builder);

            // Insert some rows and test the statistics.
            var prevStatistics = new KuduTableStatistics(-1, -1);
            var currentStatistics = new KuduTableStatistics(-1, -1);
            var session = client.NewSession();
            int num = 100;
            for (int i = 0; i < num; ++i)
            {
                // Get current table statistics.
                currentStatistics = await client.GetTableStatisticsAsync(_tableName);
                Assert.True(currentStatistics.OnDiskSize >= prevStatistics.OnDiskSize);
                Assert.True(currentStatistics.LiveRowCount >= prevStatistics.LiveRowCount);
                Assert.True(currentStatistics.LiveRowCount <= i + 1);
                prevStatistics = currentStatistics;
                // Insert row.
                var insert = ClientTestUtil.CreateBasicSchemaInsert(table, i);
                await session.EnqueueAsync(insert);
                await session.FlushAsync();
                int numRows = await ClientTestUtil.CountRowsAsync(client, table);
                Assert.Equal(i + 1, numRows);
            }

            // Final accuracy test.
            // Wait for master to aggregate table statistics.
            await Task.Delay(200 * 6);
            currentStatistics = await client.GetTableStatisticsAsync(_tableName);
            Assert.True(currentStatistics.OnDiskSize >= prevStatistics.OnDiskSize);
            Assert.True(currentStatistics.LiveRowCount >= prevStatistics.LiveRowCount);
            Assert.Equal(num, currentStatistics.LiveRowCount);
        }
    }
}
