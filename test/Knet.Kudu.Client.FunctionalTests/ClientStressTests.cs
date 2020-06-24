using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using Knet.Kudu.Client.Internal;
using McMaster.Extensions.Xunit;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ClientStressTests : IAsyncLifetime
    {
        private KuduTestHarness _harness;
        private KuduClient _client;
        private TestConnectionFactory _testConnectionFactory;
        private KuduTable _table;
        private long _sharedWriteTimestamp;

        public async Task InitializeAsync()
        {
            _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();

            var options = _harness.CreateClientBuilder().BuildOptions();
            var securityContext = new SecurityContext();
            var systemClock = new SystemClock();
            var connectionFactory = new KuduConnectionFactory(
                options, securityContext, NullLoggerFactory.Instance);
            _testConnectionFactory = new TestConnectionFactory(connectionFactory);

            _client = new KuduClient(
                options,
                securityContext,
                _testConnectionFactory,
                systemClock,
                NullLoggerFactory.Instance);

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName("chaos_test_table")
                .SetNumReplicas(3)
                .CreateBasicRangePartition();

            _table = await _client.CreateTableAsync(builder);
        }

        public async Task DisposeAsync()
        {
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        [SkippableFact]
        public async Task Test()
        {
            var testRuntime = TimeSpan.FromMinutes(1);

            using var cts = new CancellationTokenSource(testRuntime);
            var token = cts.Token;

            var chaosTask = RunTaskAsync(() => DoChaosAsync(token), cts);
            var writeTask = RunTaskAsync(() => WriteAsync(token), cts);
            var scanTask = RunTaskAsync(() => ScanAsync(token), cts);

            await Task.WhenAll(chaosTask, writeTask, scanTask);

            // If the test passed, do some extra validation at the end.
            int rowCount = await ClientTestUtil.CountRowsAsync(_client, _table);
            Assert.True(rowCount > 0);
        }

        private async Task RunTaskAsync(Func<Task> task, CancellationTokenSource cts)
        {
            try
            {
                await task();
            }
            catch
            {
                // Unexpected exception. Stop running the test.
                cts.Cancel();
                throw;
            }
        }

        private async Task DoChaosAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(2000);

            while (!cancellationToken.IsCancellationRequested)
            {
                int randomInt = ThreadSafeRandom.Instance.Next(3);

                if (randomInt == 0)
                {
                    await RestartTabletServerAsync();
                }
                else if (randomInt == 1)
                {
                    await _testConnectionFactory.DisconnectRandomConnectionAsync();
                }
                else
                {
                    await _harness.RestartLeaderMasterAsync();
                }

                await Task.Delay(5000);
            }
        }

        private async Task WriteAsync(CancellationToken cancellationToken)
        {
            Exception sessionException = null;
            Exception exception;

            ValueTask HandleSessionExceptionAsync(SessionExceptionContext context)
            {
                Volatile.Write(ref sessionException, context.Exception);
                return new ValueTask();
            }

            var options = new KuduSessionOptions
            {
                ExceptionHandler = HandleSessionExceptionAsync
            };
            await using var session = _client.NewSession(options);

            int currentRowKey = 0;
            bool flush = false;

            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                exception = Volatile.Read(ref sessionException);
                if (exception != null)
                    throw exception;

                var row = ClientTestUtil.CreateBasicSchemaInsert(_table, currentRowKey);
                await session.EnqueueAsync(row);

                if (flush)
                    await session.FlushAsync();

                // Every 10 rows we flush and change the flush mode randomly.
                if (currentRowKey % 10 == 0)
                    flush = ThreadSafeRandom.Instance.NextBool();

                currentRowKey++;

                Volatile.Write(ref _sharedWriteTimestamp, _client.LastPropagatedTimestamp);
            }

            await session.FlushAsync();

            exception = Volatile.Read(ref sessionException);
            if (exception != null)
                throw exception;
        }

        private async Task ScanAsync(CancellationToken cancellationToken)
        {
            int lastRowCount = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                var sharedWriteTimestamp = Volatile.Read(ref _sharedWriteTimestamp);

                if (sharedWriteTimestamp == 0)
                {
                    // Nothing has been written yet.
                }
                else if (
                    lastRowCount == 0 || // Need to full scan once before random reading.
                    ThreadSafeRandom.Instance.NextBool())
                {
                    lastRowCount = await FullScanAsync(lastRowCount);
                }
                else
                {
                    await RandomGetAsync(lastRowCount);
                }

                if (lastRowCount == 0)
                    await Task.Delay(50);
            }
        }

        private async Task<int> FullScanAsync(int previousRows)
        {
            var scanner = GetScannerBuilder().Build();
            var numRows = await ClientTestUtil.CountRowsInScanAsync(scanner);

            if (numRows < previousRows)
                throw new Exception($"Row count unexpectedly decreased from {previousRows} to {numRows}");

            return numRows;
        }

        private async Task RandomGetAsync(int lastRowCount)
        {
            // Read a row at random that should exist (smaller than lastRowCount).
            int key = ThreadSafeRandom.Instance.Next(lastRowCount);
            var columnName = _table.Schema.GetColumn(0).Name;

            var scanner = GetScannerBuilder()
                .AddComparisonPredicate(columnName, ComparisonOp.Equal, key)
                .Build();

            var results = new List<int>();

            await foreach (var resultSet in scanner)
            {
                AccumulateResults(resultSet);

                void AccumulateResults(ResultSet resultSet)
                {
                    foreach (var row in resultSet)
                    {
                        results.Add(row.GetInt32(0));
                    }
                }
            }

            var result = Assert.Single(results);
            Assert.Equal(key, result);
        }

        private KuduScannerBuilder GetScannerBuilder()
        {
            var sharedWriteTimestamp = Volatile.Read(ref _sharedWriteTimestamp);

            return _client.NewScanBuilder(_table)
                .SetReadMode(ReadMode.ReadAtSnapshot)
                .SnapshotTimestampRaw(sharedWriteTimestamp)
                .SetFaultTolerant(true);
        }

        private async ValueTask RestartTabletServerAsync()
        {
            var tablets = await _client.GetTableLocationsAsync(_table.TableId, null, 100);
            Assert.NotEmpty(tablets);

            var random = ThreadSafeRandom.Instance;

            // Pick a random tablet from the table.
            var tablet = tablets[random.Next(tablets.Count)];

            // Pick a random replica from the tablet.
            var replica = tablet.Replicas[random.Next(tablet.Replicas.Count)];

            await _harness.KillTabletServerAsync(replica.HostPort);
            await _harness.StartTabletServerAsync(replica.HostPort);
        }

        private class TestConnectionFactory : IKuduConnectionFactory
        {
            private readonly IKuduConnectionFactory _realConnectionFactory;
            private readonly ConcurrentDictionary<IPEndPoint, Task<KuduConnection>> _cache;

            public TestConnectionFactory(IKuduConnectionFactory realConnectionFactory)
            {
                _realConnectionFactory = realConnectionFactory;
                _cache = new ConcurrentDictionary<IPEndPoint, Task<KuduConnection>>();
            }

            public Task<KuduConnection> ConnectAsync(ServerInfo serverInfo, CancellationToken cancellationToken = default)
            {
                var connectionTask = _realConnectionFactory.ConnectAsync(serverInfo, cancellationToken);
                _cache[serverInfo.Endpoint] = connectionTask;
                return connectionTask;
            }

            public Task<ServerInfo> GetServerInfoAsync(string uuid, string location, HostAndPort hostPort)
            {
                return _realConnectionFactory.GetServerInfoAsync(uuid, location, hostPort);
            }

            public async Task DisconnectRandomConnectionAsync()
            {
                var connections = _cache.Values.ToList();
                var numConnections = connections.Count;

                if (numConnections == 0)
                    return;

                var randomIndex = ThreadSafeRandom.Instance.Next(numConnections);
                var connectionTask = connections[randomIndex];

                try
                {
                    var connection = await connectionTask;
                    await connection.CloseAsync();
                }
                catch { }
            }
        }
    }
}
