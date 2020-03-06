using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ScanTokenTests : IAsyncLifetime
    {
        private readonly string _tableName = "TestScanToken";
        private readonly KuduTestHarness _harness;
        private readonly KuduClient _client;
        private readonly IKuduSession _session;

        public ScanTokenTests()
        {
            _harness = new MiniKuduClusterBuilder().BuildHarness();
            _client = _harness.CreateClient();
            _session = _client.NewSession();
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            await _session.DisposeAsync();
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        /// <summary>
        /// Tests scan tokens by creating a set of scan tokens, serializing them, and
        /// then executing them in parallel with separate client instances. This
        /// simulates the normal usecase of scan tokens being created at a central
        /// planner and distributed to remote task executors.
        /// </summary>
        [SkippableFact]
        public async Task TestScanTokens()
        {
            // For this test, make sure that we cover the case that not all tablets
            // are returned in a single batch.

            var builder = ClientTestUtil.CreateManyStringsSchema()
                .SetTableName(_tableName)
                .AddHashPartitions(8, "key")
                .CreateBasicRangePartition()
                .AddSplitRow(row => row.SetString("key", "key_50"));

            var table = await _client.CreateTableAsync(builder);

            for (int i = 0; i < 100; i++)
            {
                var row = table.NewInsert();
                row.SetString("key", $"key_{i}");
                row.SetString("c1", "c1_" + i);
                row.SetString("c2", "c2_" + i);

                await _session.EnqueueAsync(row);
            }

            await _session.FlushAsync();

            var tokenBuilder = _client.NewScanTokenBuilder(table)
                .SetEmptyProjection()
                // For this test, make sure that we cover the case that not all tablets
                // are returned in a single batch.
                .SetFetchTabletsPerRangeLookup(4);

            List<KuduScanToken> tokens = await tokenBuilder.BuildAsync();
            Assert.Equal(16, tokens.Count);

            await using var newClient = _harness.CreateClient();
            var tasks = new List<Task<int>>();

            foreach (var token in tokens)
            {
                var task = Task.Run(async () =>
                {
                    var count = 0;
                    var tokenBytes = token.Serialize();

                    var scanner = _client.NewScanBuilder(table)
                        .ApplyScanToken(tokenBytes)
                        .Build();

                    await foreach (var resultSet in scanner)
                    {
                        count += resultSet.Count;
                    }

                    return count;
                });

                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks);
            var rowCount = results.Sum();

            Assert.Equal(100, rowCount);
        }
    }
}
