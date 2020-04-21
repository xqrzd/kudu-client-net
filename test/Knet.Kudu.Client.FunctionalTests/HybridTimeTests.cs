using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using Knet.Kudu.Client.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    /// <summary>
    /// Tests client propagated timestamps. All the work for commit wait is done
    /// and tested on the server-side, so it is not tested here.
    /// </summary>
    [MiniKuduClusterTest]
    public class HybridTimeTests : IAsyncLifetime
    {
        private KuduTestHarness _harness;
        private KuduClient _client;
        private KuduTable _table;

        public async Task InitializeAsync()
        {
            _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
            _client = _harness.CreateClient();

            // Use one tablet because multiple tablets don't work: we could jump
            // from one tablet to another which could change the logical clock.
            var builder = new TableBuilder("HybridTimeTest")
                .AddColumn("key", KuduType.String, opt => opt.Key(true))
                .SetRangePartitionColumns("key");

            _table = await _client.CreateTableAsync(builder);
        }

        public async Task DisposeAsync()
        {
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        [SkippableFact]
        public async Task TestHybridTime()
        {
            // Perform one write so we receive a timestamp from the server and can use
            // it to propagate a modified timestamp back to the server. Following writes
            // should force the servers to update their clocks to this value and increment
            // the logical component of the timestamp.
            await InsertRowAsync("0");
            Assert.NotEqual(KuduClient.NoTimestamp, _client.LastPropagatedTimestamp);
            var (timestampMicros, logicalValue) = HybridTimeUtil.HtTimestampToPhysicalAndLogical(
                _client.LastPropagatedTimestamp);
            Assert.Equal(0, logicalValue);
            long futureTs = timestampMicros + 5000000;
            _client.LastPropagatedTimestamp = HybridTimeUtil.ClockTimestampToHtTimestamp(futureTs);

            var logicalValues = new List<long>();

            var keys = new[] { "1", "2", "3", "11", "22", "33" };
            for (int i = 0; i < keys.Length; i++)
            {
                await InsertRowAsync(keys[i]);
                Assert.NotEqual(KuduClient.NoTimestamp, _client.LastPropagatedTimestamp);
                (timestampMicros, logicalValue) = HybridTimeUtil.HtTimestampToPhysicalAndLogical(
                    _client.LastPropagatedTimestamp);
                Assert.Equal(futureTs, timestampMicros);
                logicalValues.Add(logicalValue);
                Assert.Equal(logicalValues.OrderBy(v => v), logicalValues);
            }

            // Scan all rows with READ_LATEST (the default), which should retrieve all rows.
            var scanner = _client.NewScanBuilder(_table)
                .SetReadMode(ReadMode.ReadLatest)
                .Build();
            Assert.Equal(1 + keys.Length, await ClientTestUtil.CountRowsInScanAsync(scanner));

            // Now scan at multiple snapshots with READ_AT_SNAPSHOT. The logical timestamp
            // from the 'i'th row (counted from 0) combined with the latest physical timestamp
            // should observe 'i + 1' rows.
            for (int i = 0; i < logicalValues.Count; i++)
            {
                logicalValue = logicalValues[i];
                long snapshotTime = HybridTimeUtil.PhysicalAndLogicalToHtTimestamp(
                    futureTs, logicalValue);
                int expected = i + 1;
                int numRows = await ScanAtSnapshotAsync(snapshotTime);
                Assert.Equal(expected, numRows);
            }

            // The last snapshots needs to be one into the future w.r.t. the last write's
            // timestamp to get all rows, but the snapshot timestamp can't be bigger than
            // the propagated timestamp. Ergo increase the propagated timestamp first.
            long latestLogicalValue = logicalValues[^1];
            _client.LastPropagatedTimestamp++;
            long snapshotTime2 = HybridTimeUtil.PhysicalAndLogicalToHtTimestamp(
                futureTs, latestLogicalValue + 1);
            int numRows2 = await ScanAtSnapshotAsync(snapshotTime2);
            Assert.Equal(1 + keys.Length, numRows2);
        }

        private Task<int> ScanAtSnapshotAsync(long time)
        {
            var scanner = _client.NewScanBuilder(_table)
                .SnapshotTimestampRaw(time)
                .SetReadMode(ReadMode.ReadAtSnapshot)
                .Build();

            return ClientTestUtil.CountRowsInScanAsync(scanner);
        }

        private async Task InsertRowAsync(string key)
        {
            var insert = _table.NewInsert();
            insert.SetString(0, key);
            await _client.WriteAsync(new[] { insert });
        }
    }
}
