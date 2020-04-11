﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ScanMultiTabletTests : IAsyncLifetime
    {
        private readonly string _tableName = "ScanMultiTabletTests";
        private readonly KuduTestHarness _harness;
        private KuduClient _client;
        private IKuduSession _session;

        private KuduTable _table;

        /// <summary>
        /// The timestamp after inserting the rows into the test table during setUp().
        /// </summary>
        private long _beforeWriteTimestamp;

        public ScanMultiTabletTests()
        {
            _harness = new MiniKuduClusterBuilder().BuildHarness();
        }

        public async Task InitializeAsync()
        {
            await using var client = _harness.CreateClient();
            await using var session = client.NewSession();

            // Create a 4-tablets table for scanning.
            var builder = new TableBuilder(_tableName)
                .AddColumn("key1", KuduType.String, opt => opt.Key(true))
                .AddColumn("key2", KuduType.String, opt => opt.Key(true))
                .AddColumn("val", KuduType.String)
                .SetRangePartitionColumns("key1", "key2");

            for (int i = 1; i < 4; i++)
            {
                builder.AddSplitRow(splitRow =>
                {
                    splitRow.SetString("key1", i.ToString());
                    splitRow.SetString("key2", "");
                });
            }

            var table = await client.CreateTableAsync(builder);

            // The data layout ends up like this:
            // tablet '', '1': no rows
            // tablet '1', '2': '111', '122', '133'
            // tablet '2', '3': '211', '222', '233'
            // tablet '3', '': '311', '322', '333'
            var keys = new[] { "1", "2", "3" };
            foreach (var key1 in keys)
            {
                foreach (var key2 in keys)
                {
                    var insert = table.NewInsert();
                    insert.SetString(0, key1);
                    insert.SetString(1, key2);
                    insert.SetString(2, key2);
                    await session.EnqueueAsync(insert);
                    await session.FlushAsync();
                }
            }

            _beforeWriteTimestamp = client.LastPropagatedTimestamp;

            // Reset the client in order to clear the propagated timestamp.
            _client = _harness.CreateClient();
            _session = _client.NewSession();

            // Reopen the table using the new client.
            _table = await _client.OpenTableAsync(_tableName);
        }

        public async Task DisposeAsync()
        {
            if (_session != null)
                await _session.DisposeAsync();

            if (_client != null)
                await _client.DisposeAsync();

            await _harness.DisposeAsync();
        }

        // TODO: Test scan metrics when they're available

        /// <summary>
        /// Test various combinations of start/end row keys.
        /// </summary>
        [SkippableFact]
        public async Task TestKeyStartEnd()
        {
            Assert.Equal(0, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("", "", "1", ""))); // There's nothing in the 1st tablet
            Assert.Equal(1, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("", "", "1", "2"))); // Grab the very first row
            Assert.Equal(3, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "1", "1", "4"))); // Grab the whole 2nd tablet
            Assert.Equal(3, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "1", "2", ""))); // Same, and peek at the 3rd
            Assert.Equal(3, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "1", "2", "0"))); // Same, different peek
            Assert.Equal(4, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "2", "2", "3"))); // Middle of 2nd to middle of 3rd
            Assert.Equal(3, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "4", "2", "4"))); // Peek at the 2nd then whole 3rd
            Assert.Equal(6, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "5", "3", "4"))); // Whole 3rd and 4th
            Assert.Equal(9, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("", "", "4", ""))); // Full table scan
            Assert.Equal(9, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("", "", null, null))); // Full table scan with empty upper
            Assert.Equal(9, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner(null, null, "4", ""))); // Full table scan with empty lower
            Assert.Equal(9, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner(null, null, null, null))); // Full table scan with empty bounds

            // Test that we can close a scanner while in between two tablets. We start on the second
            // tablet and our first nextRows() will get 3 rows. At that moment we want to close the scanner
            // before getting on the 3rd tablet.
            var scanner = GetScanner("1", "", null, null);
            var scanEnumerator = scanner.GetAsyncEnumerator();
            Assert.True(await scanEnumerator.MoveNextAsync());
            var resultSet = scanEnumerator.Current;
            Assert.Equal(3, resultSet.Count);
            await scanEnumerator.DisposeAsync();
            Assert.False(await scanEnumerator.MoveNextAsync());
        }

        private KuduScanner<ResultSet> GetScanner(
            string lowerBoundKeyOne,
            string lowerBoundKeyTwo,
            string exclusiveUpperBoundKeyOne,
            string exclusiveUpperBoundKeyTwo)
        {
            var builder = _client.NewScanBuilder(_table);

            if (lowerBoundKeyOne != null)
            {
                var lowerBoundRow = new PartialRow(_table.Schema);
                lowerBoundRow.SetString(0, lowerBoundKeyOne);
                lowerBoundRow.SetString(1, lowerBoundKeyTwo);
                builder.LowerBound(lowerBoundRow);
            }

            if (exclusiveUpperBoundKeyOne != null)
            {
                var upperBoundRow = new PartialRow(_table.Schema);
                upperBoundRow.SetString(0, exclusiveUpperBoundKeyOne);
                upperBoundRow.SetString(1, exclusiveUpperBoundKeyTwo);
                builder.ExclusiveUpperBound(upperBoundRow);
            }

            return builder.Build();
        }
    }
}
