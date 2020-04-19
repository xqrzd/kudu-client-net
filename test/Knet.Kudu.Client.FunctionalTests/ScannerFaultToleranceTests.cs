using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ScannerFaultToleranceTests : IAsyncLifetime
    {
        private readonly int _numTablets = 3;
        private readonly int _numRows = 20000;

        private readonly Random _random;

        private KuduTestHarness _harness;
        private KuduClient _client;
        private IKuduSession _session;

        private KuduTable _table;
        private HashSet<int> _keys;

        public ScannerFaultToleranceTests()
        {
            _random = new Random();
        }

        public async Task InitializeAsync()
        {
            _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
            _client = _harness.CreateClient();
            _session = _client.NewSession();

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName("ScannerFaultToleranceTests")
                .AddHashPartitions(_numTablets, "key");

            _table = await _client.CreateTableAsync(builder);

            _keys = new HashSet<int>();
            while (_keys.Count < _numRows)
            {
                _keys.Add(_random.Next());
            }

            int i = 0;
            foreach (var key in _keys)
            {
                var insert = _table.NewInsert();
                insert.SetInt32(0, key);
                insert.SetInt32(1, i);
                insert.SetInt32(2, i++);
                insert.SetString(3, DataGenerator.RandomString(1024, _random));
                insert.SetBool(4, true);

                await _session.EnqueueAsync(insert);
            }

            await _session.FlushAsync();
        }

        public async Task DisposeAsync()
        {
            await _session.DisposeAsync();
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        [SkippableTheory]
        // Tests non fault tolerant scanner by killing the tablet server while scanning.
        [InlineData(false, false)]
        // Tests non fault tolerant scanner by restarting the tablet server while scanning.
        [InlineData(true, false)]
        public async Task TestNonFaultTolerantScanner(bool restart, bool finishFirstScan)
        {
            await Assert.ThrowsAsync<NonRecoverableException>(
                () => TestServerFaultInjectionAsync(restart, false, finishFirstScan));
        }

        [SkippableTheory]
        // Tests fault tolerant scanner by restarting the tablet server in the middle
        // of tablet scanning and verifies the scan results are as expected.
        [InlineData(true, false)]
        // Tests fault tolerant scanner by killing the tablet server in the middle
        // of tablet scanning and verifies the scan results are as expected.
        [InlineData(false, false)]
        // Tests fault tolerant scanner by killing the tablet server while scanning
        // (after finish scan of first tablet) and verifies the scan results are as expected.
        [InlineData(false, true)]
        // Tests fault tolerant scanner by restarting the tablet server while scanning
        // (after finish scan of first tablet) and verifies the scan results are as expected.
        [InlineData(true, true)]
        public async Task TestFaultTolerantScanner(bool restart, bool finishFirstScan)
        {
            await TestServerFaultInjectionAsync(restart, true, finishFirstScan);
        }

        /// <summary>
        /// Verifies for fault tolerant scanner, it can proceed
        /// properly even if disconnects client connection.
        /// </summary>
        [SkippableFact]
        public async Task TestFaultTolerantDisconnect()
        {
            await TestClientFaultInjectionAsync(isFaultTolerant: true);
        }

        /// <summary>
        /// Verifies for non fault tolerant scanner, it can proceed
        /// properly even if shuts down client connection.
        /// </summary>
        [SkippableFact]
        public async Task TestNonFaultTolerantDisconnect()
        {
            await TestClientFaultInjectionAsync(isFaultTolerant: false);
        }

        /// <summary>
        /// Injecting failures (kill or restart TabletServer) while scanning, to verify:
        /// fault tolerant scanner will continue scan and non-fault tolerant scanner will
        /// throw <see cref="NonRecoverableException"/>. Also makes sure we pass all the
        /// correct information down to the server by verifying we get rows in order from
        /// 3 tablets. We detect those tablet boundaries when keys suddenly become smaller
        /// than what was previously seen.
        /// </summary>
        /// <param name="restart">
        /// If true restarts TabletServer, otherwise kills TabletServer.
        /// </param>
        /// <param name="isFaultTolerant">
        /// If true uses fault tolerant scanner, otherwise uses non fault-tolerant one.
        /// </param>
        /// <param name="finishFirstScan">
        /// If true injects failure before finishing first tablet scan, otherwise in the
        /// middle of tablet scanning.
        /// </param>
        private async Task TestServerFaultInjectionAsync(
            bool restart,
            bool isFaultTolerant,
            bool finishFirstScan)
        {
            var scanner = _client.NewScanBuilder(_table)
                .SetFaultTolerant(isFaultTolerant)
                .SetBatchSizeBytes(1)
                .SetProjectedColumns(0)
                .Build();

            await using var scanEnumerator = scanner.GetAsyncEnumerator();

            int previousRow = -1;
            var keys = new List<int>();

            if (await scanEnumerator.MoveNextAsync())
            {
                var resultSet = scanEnumerator.Current;
                var results = ResultSetToKeys(resultSet);

                keys.AddRange(results);
                previousRow = keys[^1];
            }

            if (!finishFirstScan)
            {
                if (restart)
                    await _harness.RestartTabletServerAsync(scanEnumerator.Tablet);
                else
                    await _harness.KillTabletLeaderAsync(scanEnumerator.Tablet);
            }

            bool failureInjected = false;

            while (await scanEnumerator.MoveNextAsync())
            {
                var resultSet = scanEnumerator.Current;
                var results = ResultSetToKeys(resultSet);

                keys.AddRange(results);

                foreach (var key in results)
                {
                    if (key < previousRow)
                    {
                        if (finishFirstScan && !failureInjected)
                        {
                            if (restart)
                                await _harness.RestartTabletServerAsync(scanEnumerator.Tablet);
                            else
                                await _harness.KillTabletLeaderAsync(scanEnumerator.Tablet);

                            failureInjected = true;
                        }
                    }

                    previousRow = key;
                }
            }

            Assert.Equal(
                _keys.OrderBy(k => k),
                keys.OrderBy(k => k));
        }

        /// <summary>
        /// Injecting failures (i.e. drop client connection) while scanning, to verify:
        /// both non-fault tolerant scanner and fault tolerant scanner will continue scan
        /// as expected.
        /// </summary>
        /// <param name="isFaultTolerant">
        /// If true use fault-tolerant scanner, otherwise use non-fault-tolerant one.
        /// </param>
        private async Task TestClientFaultInjectionAsync(bool isFaultTolerant)
        {
            var scanner = _client.NewScanBuilder(_table)
                .SetFaultTolerant(isFaultTolerant)
                .SetBatchSizeBytes(1)
                .Build();

            await using var scanEnumerator = scanner.GetAsyncEnumerator();

            int rowCount = 0;
            int loopCount = 0;
            bool shouldClose = true;

            while (await scanEnumerator.MoveNextAsync())
            {
                loopCount++;
                rowCount += scanEnumerator.Current.Count;

                if (shouldClose)
                {
                    var serverInfo = scanEnumerator.Tablet.GetServerInfo(
                    scanner.ReplicaSelection);

                    var connection = await GetConnectionAsync(serverInfo);
                    await connection.StopAsync();

                    shouldClose = false;
                }
            }

            Assert.True(loopCount > _numTablets);
            Assert.Equal(_numRows, rowCount);
        }

        private List<int> ResultSetToKeys(ResultSet resultSet)
        {
            var results = new List<int>(resultSet.Count);
            foreach (var row in resultSet)
            {
                results.Add(row.GetInt32(0));
            }
            return results;
        }

        private Task<KuduConnection> GetConnectionAsync(ServerInfo serverInfo)
        {
            // TODO: Don't use reflection here, allow IConnectionFactory to be passed in.
            var connectionFactoryField = typeof(KuduClient).GetField("_connectionFactory",
                BindingFlags.NonPublic | BindingFlags.Instance);

            var connectionFactory = (IKuduConnectionFactory)connectionFactoryField.GetValue(_client);

            return connectionFactory.ConnectAsync(serverInfo);
        }
    }
}
