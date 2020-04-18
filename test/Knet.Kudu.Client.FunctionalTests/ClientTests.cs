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
        private readonly KuduTestHarness _harness;
        private readonly KuduClient _client;

        public ClientTests()
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

        /// <summary>
        /// Stress test which performs upserts from many sessions on different threads
        /// sharing the same KuduClient and KuduTable instance.
        /// </summary>
        [SkippableFact]
        public async Task TestMultipleSessions()
        {
            var testRunTime = TimeSpan.FromSeconds(5);
            int numTasks = 60;
            var tasks = new List<Task>(numTasks);
            bool stillRunning = true;

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName("TestMultipleSessions")
                .CreateBasicRangePartition();

            var table = await _client.CreateTableAsync(builder);

            for (int i = 0; i < numTasks; i++)
            {
                var task = Task.Run(async () =>
                {
                    while (Volatile.Read(ref stillRunning))
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

            await Task.Delay(testRunTime);
            Volatile.Write(ref stillRunning, false);

            await Task.WhenAll(tasks);
        }
    }
}
