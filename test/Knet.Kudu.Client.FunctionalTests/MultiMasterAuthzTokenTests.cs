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
        private readonly KuduTestHarness _harness;
        private readonly KuduClient _client;

        public MultiMasterAuthzTokenTests()
        {
            _harness = new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--authz_token_validity_seconds=1")
                .AddTabletServerFlag("--tserver_enforce_access_control=true")
                // Inject invalid tokens such that operations will be forced to go
                // back to the master for an authz token.
                .AddTabletServerFlag("--tserver_inject_invalid_authz_token_ratio=0.5")
                .BuildHarness();

            _client = _harness.CreateClient();
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        [SkippableFact]
        public async Task TestAuthzTokensDuringElection()
        {
            await using var session = _client.NewSession();

            // Test sending various requests that require authorization.
            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .CreateBasicRangePartition()
                .SetNumReplicas(1);

            var table = await _client.CreateTableAsync(builder);

            // Restart the masters to trigger an election.
            _harness.KillAllMasterServers();
            _harness.StartAllMasterServers();

            int numReqs = 10;
            await InsertRowsAsync(session, table, 0, numReqs);
            await session.FlushAsync();

            // Do the same for batches of inserts.
            _harness.KillAllMasterServers();
            _harness.StartAllMasterServers();
            await InsertRowsAsync(session, table, numReqs, numReqs);
            await session.FlushAsync();

            // And for scans.
            _harness.KillAllMasterServers();
            _harness.StartAllMasterServers();
            for (int i = 0; i < numReqs; i++)
            {
                var numRows = await ClientTestUtil.CountRowsAsync(_client, table);
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

            var testRuntime = TimeSpan.FromSeconds(10);

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .CreateBasicRangePartition()
                .SetNumReplicas(1);

            var table = await _client.CreateTableAsync(builder);

            long keepRunning = 1;
            int rowStart = 0;

            var loop1 = WriteRowsAsync(flush: true);
            var loop2 = WriteRowsAsync(flush: false);
            var scanTask = ScanTableAsync();

            await Task.Delay(testRuntime);
            Interlocked.Decrement(ref keepRunning);

            await loop1;
            await loop2;
            await scanTask;

            var expected = Interlocked.Add(ref rowStart, 0) * 10;
            var rows = await ClientTestUtil.CountRowsAsync(_client, table);
            Assert.Equal(expected, rows);

            async Task WriteRowsAsync(bool flush)
            {
                await using var session = _client.NewSession();

                while (Interlocked.Read(ref keepRunning) == 1)
                {
                    var start = Interlocked.Increment(ref rowStart) * 10;
                    await InsertRowsAsync(session, table, start, 10);

                    if (flush)
                        await session.FlushAsync();
                }
            }

            async Task ScanTableAsync()
            {
                while (Interlocked.Read(ref keepRunning) == 1)
                {
                    // We can't validate the row count until the end, but this
                    // still ensures the scanner doesn't throw any exceptions.
                    await ClientTestUtil.CountRowsAsync(_client, table);
                }
            }
        }

        private async ValueTask InsertRowsAsync(
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
