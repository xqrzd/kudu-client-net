using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class HandleTooBusyTests
    {
        /// <summary>
        /// Provoke overflows in the master RPC queue while connecting to the master
        /// and performing location lookups.
        /// </summary>
        [SkippableFact]
        public async Task TestMasterLookupOverflow()
        {
            var tableName = "TestHandleTooBusy";

            await using var harness = await new MiniKuduClusterBuilder()
                // Short queue to provoke overflow.
                .AddMasterServerFlag("--rpc_service_queue_length=1")
                // Low number of service threads, so things stay in the queue.
                .AddMasterServerFlag("--rpc_num_service_threads=3")
                // Inject latency so lookups process slowly.
                .AddMasterServerFlag("--master_inject_latency_on_tablet_lookups_ms=100")
                .BuildHarnessAsync();

            await using var client1 = harness.CreateClient();

            await client1.CreateTableAsync(ClientTestUtil.GetBasicSchema()
                .SetTableName(tableName));

            var tasks = new List<Task>();

            for (int i = 0; i < 10; i++)
            {
                var task = Task.Run(async () =>
                {
                    for (int j = 0; j < 5; j++)
                    {
                        await using var client = harness.CreateClient();
                        var table = await client.OpenTableAsync(tableName);

                        for (int k = 0; k < 5; k++)
                        {
                            await client.GetTableLocationsAsync(
                                table.TableId, Array.Empty<byte>(), 1);
                        }
                    }
                });

                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }
}
