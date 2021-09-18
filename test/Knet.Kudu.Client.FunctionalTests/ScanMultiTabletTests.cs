using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using Knet.Kudu.Client.Protobuf.Client;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class ScanMultiTabletTests : IAsyncLifetime
{
    private readonly string _tableName = "ScanMultiTabletTests";
    private KuduTestHarness _harness;
    private KuduClient _client;
    private KuduClient _newClient;

    private KuduTable _table;
    private KuduSchema _schema;

    /// <summary>
    /// The timestamp after inserting the rows into the test table during initialize.
    /// </summary>
    private long _beforeWriteTimestamp;

    public async Task InitializeAsync()
    {
        _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
        _client = _harness.CreateClient();
        await using var session = _client.NewSession();

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

        var table = await _client.CreateTableAsync(builder);

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

        _beforeWriteTimestamp = _client.LastPropagatedTimestamp;

        // Reset the client in order to clear the propagated timestamp.
        _newClient = _harness.CreateClient();

        // Reopen the table using the new client.
        _table = await _newClient.OpenTableAsync(_tableName);
        _schema = _table.Schema;
    }

    public async Task DisposeAsync()
    {
        if (_client is not null)
            await _client.DisposeAsync();

        if (_newClient is not null)
            await _newClient.DisposeAsync();

        await _harness.DisposeAsync();
    }

    /// <summary>
    /// Test scanner resource metrics.
    /// </summary>
    [SkippableFact]
    public async Task TestResourceMetrics()
    {
        // Scan one tablet and the whole table.
        var oneTabletScanner = GetScanner("1", "1", "1", "4"); // Whole second tablet.
        await using var oneTabletScanEnumerator = oneTabletScanner.GetAsyncEnumerator();
        Assert.Equal(3, await ClientTestUtil.CountRowsInScanAsync(oneTabletScanEnumerator));

        var fullTableScanner = GetScanner(null, null, null, null);
        await using var fullTableScanEnumerator = fullTableScanner.GetAsyncEnumerator();
        Assert.Equal(9, await ClientTestUtil.CountRowsInScanAsync(fullTableScanEnumerator));

        // Both scans should take a positive amount of wait duration, total duration,
        // cpu user and cpu system time.
        ValidateResourceMetrics(oneTabletScanEnumerator.ResourceMetrics);
        ValidateResourceMetrics(fullTableScanEnumerator.ResourceMetrics);

        static void ValidateResourceMetrics(ResourceMetrics resourceMetrics)
        {
            Assert.True(resourceMetrics.QueueDurationNanos > 0);
            Assert.True(resourceMetrics.TotalDurationNanos > 0);
        }
    }

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
        // the second tablet and our first ResultSet will get 3 rows. At that moment
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
    public void TestProjections()
    {
        // Test with column names.
        var builder = _newClient.NewScanBuilder(_table)
            .SetProjectedColumns("key1", "key2");
        BuildScannerAndCheckColumnsCount(builder, "key1", "key2");

        // Test with column indexes.
        builder = _newClient.NewScanBuilder(_table)
            .SetProjectedColumns(0, 1);
        BuildScannerAndCheckColumnsCount(builder, "key1", "key2");

        // Test with column names overriding indexes.
        builder = _newClient.NewScanBuilder(_table)
           .SetProjectedColumns(0, 1)
           .SetProjectedColumns("key1");
        BuildScannerAndCheckColumnsCount(builder, "key1");

        // Test with keys last with indexes.
        builder = _newClient.NewScanBuilder(_table)
           .SetProjectedColumns(2, 1, 0);
        BuildScannerAndCheckColumnsCount(builder, "val", "key2", "key1");

        // Test with keys last with column names.
        builder = _newClient.NewScanBuilder(_table)
           .SetProjectedColumns("val", "key1");
        BuildScannerAndCheckColumnsCount(builder, "val", "key1");
    }

    [SkippableTheory]
    [InlineData(ReplicaSelection.LeaderOnly)]
    [InlineData(ReplicaSelection.ClosestReplica)]
    public async Task TestReplicaSelections(ReplicaSelection replicaSelection)
    {
        var scanner = _newClient.NewScanBuilder(_table)
            .SetReplicaSelection(replicaSelection)
            .Build();

        await ClientTestUtil.WaitUntilRowCountAsync(scanner, 9);
    }

    [SkippableFact]
    public async Task TestScanTokenReplicaSelections()
    {
        var tokens = await _newClient.NewScanTokenBuilder(_table)
            .SetReplicaSelection(ReplicaSelection.ClosestReplica)
            .BuildAsync();

        var totalRows = await ClientTestUtil.WaitUntilAsync(rows => rows == 9, async () =>
        {
            long rows = 0;

            foreach (var token in tokens)
            {
                var serializedToken = token.Serialize();

                // Deserialize the scan token into a scanner, and make sure it is using
                // 'CLOSEST_REPLICA' selection policy.
                var scanBuilder = await _newClient.NewScanBuilderFromTokenAsync(serializedToken);
                var scanner = scanBuilder.Build();

                Assert.Equal(ReplicaSelection.ClosestReplica, scanner.ReplicaSelection);
                rows += await ClientTestUtil.CountRowsInScanAsync(scanner);
            }

            return rows;
        });

        Assert.Equal(9, totalRows);
    }

    [SkippableFact]
    public async Task TestReadAtSnapshotNoTimestamp()
    {
        // Perform scan in READ_AT_SNAPSHOT mode with no snapshot timestamp
        // specified. Verify that the scanner timestamp is set from the tablet
        // server response.

        var scanner = _newClient.NewScanBuilder(_table)
            .SetReadMode(ReadMode.ReadAtSnapshot)
            .Build();

        await using var scanEnumerator = scanner.GetAsyncEnumerator();

        Assert.Equal(ReadMode.ReadAtSnapshot, scanner.ReadMode);
        Assert.Equal(KuduClient.NoTimestamp, scanEnumerator.SnapshotTimestamp);

        Assert.True(await scanEnumerator.MoveNextAsync());
        // At this point, the call to the first tablet server should have been
        // done already, so check the snapshot timestamp.
        var tsRef = scanEnumerator.SnapshotTimestamp;
        Assert.NotEqual(KuduClient.NoTimestamp, tsRef);

        var rowCount = scanEnumerator.Current.Count;
        while (await scanEnumerator.MoveNextAsync())
        {
            rowCount += scanEnumerator.Current.Count;
            Assert.Equal(tsRef, scanEnumerator.SnapshotTimestamp);
        }

        Assert.Equal(9, rowCount);
    }

    /// <summary>
    /// Regression test for KUDU-2415.
    /// Scanning a never-written-to tablet from a fresh client with no propagated
    /// timestamp in "read-your-writes' mode should not fail.
    /// </summary>
    [SkippableFact]
    public async Task TestReadYourWritesFreshClientFreshTable()
    {
        // Perform scan in READ_YOUR_WRITES mode. Before the scan, verify that the
        // propagated timestamp is unset, since this is a fresh client.
        var scanner = _newClient.NewScanBuilder(_table)
            .SetReadMode(ReadMode.ReadYourWrites)
            .Build();

        await using var scanEnumerator = scanner.GetAsyncEnumerator();

        Assert.Equal(ReadMode.ReadYourWrites, scanner.ReadMode);
        Assert.Equal(KuduClient.NoTimestamp, _newClient.LastPropagatedTimestamp);
        Assert.Equal(KuduClient.NoTimestamp, scanEnumerator.SnapshotTimestamp);

        // Since there isn't any write performed from the client, the count
        // should range from [0, 9].
        long count = 0;
        while (await scanEnumerator.MoveNextAsync())
        {
            count += scanEnumerator.Current.Count;
        }

        Assert.True(count >= 0);
        Assert.True(count <= 9);

        Assert.NotEqual(KuduClient.NoTimestamp, _newClient.LastPropagatedTimestamp);
        Assert.NotEqual(KuduClient.NoTimestamp, scanEnumerator.SnapshotTimestamp);
    }

    [SkippableFact]
    public async Task TestReadYourWrites()
    {
        long preTs = _beforeWriteTimestamp;

        // Update the propagated timestamp to ensure we see the rows written
        // in the constructor.
        _newClient.LastPropagatedTimestamp = preTs;

        // Perform scan in READ_YOUR_WRITES mode. Before the scan, verify that the
        // scanner timestamp is not yet set. It will get set only once the scan
        // is opened.
        var scanner = _newClient.NewScanBuilder(_table)
            .SetReadMode(ReadMode.ReadYourWrites)
            .Build();

        var scanEnumerator = scanner.GetAsyncEnumerator();

        Assert.Equal(ReadMode.ReadYourWrites, scanner.ReadMode);
        Assert.Equal(KuduClient.NoTimestamp, scanEnumerator.SnapshotTimestamp);

        long count = 0;
        while (await scanEnumerator.MoveNextAsync())
        {
            count += scanEnumerator.Current.Count;
        }

        Assert.Equal(9, count);

        // After the scan, verify that the chosen snapshot timestamp is
        // returned from the server and it is larger than the previous
        // propagated timestamp.
        Assert.NotEqual(KuduClient.NoTimestamp, scanEnumerator.SnapshotTimestamp);
        Assert.True(preTs < scanEnumerator.SnapshotTimestamp);
        await scanEnumerator.DisposeAsync();

        // Perform write in batch mode.
        var rows = new[] { "11", "22", "33" }.Select(key =>
        {
            var insert = _table.NewInsert();
            insert.SetString(0, key);
            insert.SetString(1, key);
            return insert;
        });

        await _newClient.WriteAsync(rows);

        scanEnumerator = scanner.GetAsyncEnumerator();

        Assert.True(preTs < _newClient.LastPropagatedTimestamp);
        preTs = _newClient.LastPropagatedTimestamp;

        count = 0;
        while (await scanEnumerator.MoveNextAsync())
        {
            count += scanEnumerator.Current.Count;
        }

        Assert.Equal(12, count);

        // After the scan, verify that the chosen snapshot timestamp is
        // returned from the server and it is larger than the previous
        // propagated timestamp.
        Assert.True(preTs < scanEnumerator.SnapshotTimestamp);
        await scanEnumerator.DisposeAsync();
    }

    [SkippableFact]
    public async Task TestScanPropagatesLatestTimestamp()
    {
        // Initially, the client does not have the timestamp set.
        Assert.Equal(KuduClient.NoTimestamp, _newClient.LastPropagatedTimestamp);

        var scanner = _newClient.NewScanBuilder(_table).Build();
        var scanEnumerator = scanner.GetAsyncEnumerator();

        Assert.True(await scanEnumerator.MoveNextAsync());
        long rowCount = scanEnumerator.Current.Count;

        // At this point, the call to the first tablet server should have been
        // done already, so the client should have received the propagated timestamp
        // in the scanner response.
        long tsRef = _newClient.LastPropagatedTimestamp;
        Assert.NotEqual(KuduClient.NoTimestamp, tsRef);

        while (await scanEnumerator.MoveNextAsync())
        {
            rowCount += scanEnumerator.Current.Count;
            var ts = _newClient.LastPropagatedTimestamp;

            // Next scan responses from tablet servers should move the propagated
            // timestamp further.
            Assert.True(ts > tsRef);
            tsRef = ts;
        }

        Assert.NotEqual(0, rowCount);
    }

    [SkippableFact]
    public async Task TestScanTokenPropagatesTimestamp()
    {
        // Initially, the client does not have the timestamp set.
        Assert.Equal(KuduClient.NoTimestamp, _newClient.LastPropagatedTimestamp);

        var scanner = _newClient.NewScanBuilder(_table).Build();
        await using var scanEnumerator = scanner.GetAsyncEnumerator();

        // Let the client receive the propagated timestamp in the scanner response.
        Assert.True(await scanEnumerator.MoveNextAsync());
        var tsPrev = _newClient.LastPropagatedTimestamp;
        var tsPropagated = tsPrev + 1000000;

        var tokenPb = new ScanTokenPB
        {
            TableName = _tableName,
            PropagatedTimestamp = (ulong)tsPropagated
        };

        var token = new KuduScanToken(null, tokenPb);
        var serializedToken = token.Serialize();

        // Deserialize scan tokens and make sure the client's last propagated
        // timestamp is updated accordingly.
        Assert.Equal(tsPrev, _newClient.LastPropagatedTimestamp);

        var scanBuilder = await _newClient.NewScanBuilderFromTokenAsync(serializedToken);
        var tokenScanner = scanBuilder.Build();

        Assert.Equal(tsPropagated, _newClient.LastPropagatedTimestamp);
    }

    [SkippableFact]
    public async Task TestScanTokenReadMode()
    {
        var tokens = await _newClient.NewScanTokenBuilder(_table)
            .SetReadMode(ReadMode.ReadYourWrites)
            .BuildAsync();

        Assert.NotEmpty(tokens);

        // Deserialize scan tokens and make sure the read mode is updated accordingly.
        foreach (var token in tokens)
        {
            var scanBuilder = await _newClient.NewScanBuilderFromTokenAsync(token);
            var scanner = scanBuilder.Build();

            Assert.Equal(ReadMode.ReadYourWrites, scanner.ReadMode);
        }
    }

    private KuduScanner GetScanner(
        string lowerBoundKeyOne,
        string lowerBoundKeyTwo,
        string exclusiveUpperBoundKeyOne,
        string exclusiveUpperBoundKeyTwo,
        params KuduPredicate[] predicates)
    {
        var builder = _client.NewScanBuilder(_table)
            .SetReadMode(ReadMode.ReadYourWrites);

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

    private static void BuildScannerAndCheckColumnsCount(
        KuduScannerBuilder builder, params string[] expectedColumnNames)
    {
        var scanner = builder.Build();
        var schema = scanner.ProjectionSchema;

        Assert.Equal(
            expectedColumnNames,
            schema.Columns.Select(c => c.Name));
    }
}
