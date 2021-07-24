using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ClientTests : IAsyncLifetime
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
        public async Task TestClusterId()
        {
            var clusterId = await _client.GetClusterIdAsync();
            Assert.NotEmpty(clusterId);

            // Test cached path.
            var cachedClusterId = await _client.GetClusterIdAsync();
            Assert.NotEmpty(cachedClusterId);
            Assert.Equal(clusterId, cachedClusterId);
        }

        /// <summary>
        /// Stress test which performs upserts from many sessions on different threads
        /// sharing the same KuduClient and KuduTable instance.
        /// </summary>
        [SkippableFact]
        public async Task TestMultipleSessions()
        {
            int numTasks = 60;
            var tasks = new List<Task>(numTasks);

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName("TestMultipleSessions")
                .CreateBasicRangePartition();

            var table = await _client.CreateTableAsync(builder);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var token = cts.Token;

            for (int i = 0; i < numTasks; i++)
            {
                var task = Task.Run(async () =>
                {
                    while (!token.IsCancellationRequested)
                    {
                        await using var session = _client.NewSession();

                        for (int j = 0; j < 100; j++)
                        {
                            var row = table.NewUpsert();
                            row.SetInt32(0, j);
                            row.SetInt32(1, 12345);
                            row.SetInt32(2, 3);
                            row.SetNull(3);
                            row.SetBool(4, false);
                            await session.EnqueueAsync(row);
                        }
                    }
                });

                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }
}
