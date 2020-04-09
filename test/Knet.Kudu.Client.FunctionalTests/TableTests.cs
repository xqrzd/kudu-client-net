using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class TableTests : IAsyncLifetime
    {
        private readonly string _tableName = "TestKuduTable";
        private readonly KuduTestHarness _harness;
        private readonly KuduClient _client;

        public TableTests()
        {
            _harness = new MiniKuduClusterBuilder().BuildHarness();
            _client = _harness.CreateClient();
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        [SkippableFact]
        public async Task TestDimensionLabel()
        {
            // Create a table with dimension label.
            var tableBuilder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .CreateBasicNonCoveredRangePartitions()
                .SetDimensionLabel("labelA");

            var table = await _client.CreateTableAsync(tableBuilder);

            // Add a range partition to the table with dimension label.
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 300);
                    upper.SetInt32("key", 400);
                }, "labelB", RangePartitionBound.Inclusive, RangePartitionBound.Exclusive));

            var tablets = await _client.GetTableLocationsAsync(table.TableId, null, 100);

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
    }
}
