using System;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class MultiMasterAuthzTokenTests : IAsyncLifetime
    {
        private readonly string _tableName = "TestMultiMasterAuthzToken-table";
        private KuduTestHarness _harness;

        public async Task InitializeAsync()
        {
            _harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--authz_token_validity_seconds=1")
                .AddTabletServerFlag("--tserver_enforce_access_control=true")
                // Inject invalid tokens such that operations will be forced to go
                // back to the master for an authz token.
                .AddTabletServerFlag("--tserver_inject_invalid_authz_token_ratio=0.5")
                .BuildHarnessAsync();
        }

        public async Task DisposeAsync()
        {
            await _harness.DisposeAsync();
        }

        [SkippableFact]
        public async Task TestAuthzTokensDuringElection()
        {
            await using var client = _harness.CreateClient();
            await using var session = client.NewSession();

            // Test sending various requests that require authorization.
            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .CreateBasicRangePartition()
                .SetNumReplicas(1);

            var table = await client.CreateTableAsync(builder);

            // Restart the masters to trigger an election.
            await _harness.KillAllMasterServersAsync();
            await _harness.StartAllMasterServersAsync();

            int numReqs = 10;
            await InsertRowsAsync(session, table, 0, numReqs);
            await session.FlushAsync();

            // Do the same for batches of inserts.
            await _harness.KillAllMasterServersAsync();
            await _harness.StartAllMasterServersAsync();
            await InsertRowsAsync(session, table, numReqs, numReqs);
            await session.FlushAsync();

            // And for scans.
            await _harness.KillAllMasterServersAsync();
            await _harness.StartAllMasterServersAsync();
            for (int i = 0; i < numReqs; i++)
            {
                var numRows = await ClientTestUtil.CountRowsAsync(client, table);
                Assert.Equal(2 * numReqs, numRows);
            }
        }

        [SkippableFact]
        public async Task TestAuthzTokenExpiration()
        {
            // Test a long-running concurrent workload with different types of requests
            // being sent, all the while injecting invalid tokens, with a short authz
            // token expiration time. The threads should reacquire tokens as needed
            // without surfacing token errors to the client.

            await using var client = _harness.CreateClientBuilder()
                .SetDefaultOperationTimeout(TimeSpan.FromMinutes(1))
                .Build();

            using var cts = new CancellationTokenSource();
            var stopToken = cts.Token;

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .CreateBasicRangePartition()
                .SetNumReplicas(1);

            var table = await client.CreateTableAsync(builder);

            cts.CancelAfter(TimeSpan.FromSeconds(10));

            int rowStart = 0;

            var loop1 = WriteRowsAsync(flush: true);
            var loop2 = WriteRowsAsync(flush: false);
            var scanTask = ScanTableAsync();

            await loop1;
            await loop2;
            await scanTask;

            var expected = Interlocked.Add(ref rowStart, 0) * 10;
            var rows = await ClientTestUtil.CountRowsAsync(client, table);
            Assert.Equal(expected, rows);

            async Task WriteRowsAsync(bool flush)
            {
                await using var session = client.NewSession();

                while (!stopToken.IsCancellationRequested)
                {
                    var start = Interlocked.Increment(ref rowStart) * 10;
                    await InsertRowsAsync(session, table, start, 10);

                    if (flush)
                        await session.FlushAsync();
                }
            }

            async Task ScanTableAsync()
            {
                while (!stopToken.IsCancellationRequested)
                {
                    // We can't validate the row count until the end, but this
                    // still ensures the scanner doesn't throw any exceptions.
                    await ClientTestUtil.CountRowsAsync(client, table);
                }
            }
        }

        private static async ValueTask InsertRowsAsync(
            IKuduSession session, KuduTable table, int startRow, int numRows)
        {
            var end = startRow + numRows;

            for (var i = startRow; i < end; i++)
            {
                var insert = ClientTestUtil.CreateBasicSchemaInsert(table, i);
                await session.EnqueueAsync(insert);
            }
        }
    }
}
