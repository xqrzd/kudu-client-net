using System;
using System.Collections.Generic;
using System.Linq;
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
        private KuduSchema _schema;

        /// <summary>
        /// The timestamp after inserting the rows into the test table during initialize.
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
            _schema = _table.Schema;
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

            // Test that we can close a scanner while in between two tablets. We start on
            // the second tablet and our first nextRows() will get 3 rows. At that moment
            // we want to close the scanner before getting on the 3rd tablet.
            var scanner = GetScanner("1", "", null, null);
            var scanEnumerator = scanner.GetAsyncEnumerator();
            Assert.True(await scanEnumerator.MoveNextAsync());
            var resultSet = scanEnumerator.Current;
            Assert.Equal(3, resultSet.Count);
            await scanEnumerator.DisposeAsync();
            Assert.False(await scanEnumerator.MoveNextAsync());
        }

        /// <summary>
        /// Test mixing start/end row keys with predicates.
        /// </summary>
        [SkippableFact]
        public async Task TestKeysAndPredicates()
        {
            KuduPredicate upperPredicate, lowerPredicate;

            // Value that doesn't exist, predicates has primary column.
            upperPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(1), ComparisonOp.LessEqual, "1");
            Assert.Equal(0, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "2", "1", "3", upperPredicate)));

            // First row from the 2nd tablet.
            lowerPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.GreaterEqual, "1");
            upperPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.LessEqual, "1");
            Assert.Equal(1, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "", "2", "", lowerPredicate, upperPredicate)));

            // All the 2nd tablet.
            lowerPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.GreaterEqual, "1");
            upperPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.LessEqual, "3");
            Assert.Equal(3, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "", "2", "", lowerPredicate, upperPredicate)));

            // Value that doesn't exist.
            lowerPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.GreaterEqual, "4");
            Assert.Equal(0, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner("1", "", "2", "", lowerPredicate)));

            // First row from every tablet.
            lowerPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.GreaterEqual, "1");
            upperPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.LessEqual, "1");
            Assert.Equal(3, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner(null, null, null, null, lowerPredicate, upperPredicate)));

            // All the rows.
            lowerPredicate = KuduPredicate.NewComparisonPredicate(
                _schema.GetColumn(2), ComparisonOp.GreaterEqual, "1");
            Assert.Equal(9, await ClientTestUtil.CountRowsInScanAsync(
                GetScanner(null, null, null, null, lowerPredicate)));
        }

        [SkippableFact]
        public async Task TestProjections()
        {
            // Test with column names.
            var builder = _client.NewScanBuilder(_table)
                .SetProjectedColumns("key1", "key2");
            await BuildScannerAndCheckColumnsCountAsync(builder, "key1", "key2");

            // Test with column indexes.
            builder = _client.NewScanBuilder(_table)
                .SetProjectedColumns(0, 1);
            await BuildScannerAndCheckColumnsCountAsync(builder, 0, 1);
        }

        private KuduScanner<ResultSet> GetScanner(
            string lowerBoundKeyOne,
            string lowerBoundKeyTwo,
            string exclusiveUpperBoundKeyOne,
            string exclusiveUpperBoundKeyTwo,
            params KuduPredicate[] predicates)
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

            foreach (var predicate in predicates)
                builder.AddPredicate(predicate);

            return builder.Build();
        }

        private async Task BuildScannerAndCheckColumnsCountAsync(
            KuduScannerBuilder builder, params string[] expectedColumnNames)
        {
            var scanner = builder.Build();
            KuduSchema schema = null;

            await foreach (var resultSet in scanner)
            {
                schema = resultSet.Schema;
            }

            Assert.Equal(
                expectedColumnNames,
                schema.Columns.Select(c => c.Name));
        }

        private async Task BuildScannerAndCheckColumnsCountAsync(
            KuduScannerBuilder builder, params int[] expectedColumnIndexes)
        {
            var scanner = builder.Build();
            KuduSchema schema = null;

            await foreach (var resultSet in scanner)
            {
                schema = resultSet.Schema;
            }

            Assert.Equal(
                expectedColumnIndexes,
                schema.Columns.Select((c, i) => i));
        }
    }
}
