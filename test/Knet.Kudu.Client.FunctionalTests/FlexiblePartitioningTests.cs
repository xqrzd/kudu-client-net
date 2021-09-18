using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class FlexiblePartitioningTests : IAsyncLifetime
{
    private KuduTestHarness _harness;
    private KuduClient _client;

    public async Task InitializeAsync()
    {
        _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
        _client = _harness.CreateClient();
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _harness.DisposeAsync();
    }

    [SkippableFact]
    public async Task TestHashBucketedTable()
    {
        var tableBuilder = CreateTableBuilder("TestHashBucketedTable")
            .AddHashPartitions(3, "a")
            .AddHashPartitions(3, 42, "b", "c");

        await TestPartitionSchemaAsync(tableBuilder);
    }

    [SkippableTheory]
    [InlineData("c", "b")]
    [InlineData("a", "c", "b")]
    public async Task TestNonDefaultRangePartitionedTable(params string[] columns)
    {
        var tableBuilder = CreateTableBuilder("TestNonDefaultRangePartitionedTable")
            .SetRangePartitionColumns(columns)
            .AddSplitRow(split => split.SetString("c", "3"))
            .AddSplitRow(split =>
            {
                split.SetString("c", "3");
                split.SetString("b", "3");
            });

        await TestPartitionSchemaAsync(tableBuilder);
    }

    [SkippableFact]
    public async Task TestHashBucketedAndRangePartitionedTable()
    {
        var tableBuilder = CreateTableBuilder("TestHashBucketedAndRangePartitionedTable")
            .AddHashPartitions(3, "a")
            .AddHashPartitions(3, 42, "b", "c")
            .SetRangePartitionColumns("c", "b")
            .AddSplitRow(split => split.SetString("c", "3"))
            .AddSplitRow(split =>
            {
                split.SetString("c", "3");
                split.SetString("b", "3");
            });

        await TestPartitionSchemaAsync(tableBuilder);
    }

    [SkippableTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TestNonCoveredRangePartitionedTable(bool addHashPartitions)
    {
        // Create a non covered range between (3, 5, 6) and (4, 0, 0)
        var tableBuilder = CreateTableBuilder("TestNonCoveredRangePartitionedTable")
            .SetRangePartitionColumns("a", "b", "c")
            .AddRangePartition((lower, upper) =>
            {
                lower.SetString("a", "0");
                lower.SetString("b", "0");
                lower.SetString("c", "0");
                upper.SetString("a", "3");
                upper.SetString("b", "5");
                upper.SetString("c", "6");
            })
            .AddRangePartition((lower, upper) =>
            {
                lower.SetString("a", "4");
                lower.SetString("b", "0");
                lower.SetString("c", "0");
                upper.SetString("a", "5");
                upper.SetString("b", "5");
                upper.SetString("c", "6");
            });

        if (addHashPartitions)
            tableBuilder.AddHashPartitions(4, "a", "b", "c");

        await TestPartitionSchemaAsync(tableBuilder);
    }

    [SkippableFact]
    public async Task TestUnpartitionedTable()
    {
        var tableBuilder = CreateTableBuilder("TestUnpartitionedTable");
        await TestPartitionSchemaAsync(tableBuilder);
    }

    private async Task TestPartitionSchemaAsync(TableBuilder tableBuilder)
    {
        var table = await _client.CreateTableAsync(tableBuilder);
        var schema = table.Schema;

        var rows = CreateRows();
        await InsertRowsAsync(table, rows);

        // Full table scan
        Assert.Equal(rows, await CollectRowsAsync(
            _client.NewScanBuilder(table).Build()));

        { // Lower bound
            var minRow = new Row("1", "3", "5");
            var lowerBound = new PartialRow(schema);
            minRow.FillPartialRow(lowerBound);

            var expected = rows
                .Where(r => r.CompareTo(minRow) >= 0)
                .ToHashSet();

            var scanner = _client.NewScanBuilder(table)
                .LowerBound(lowerBound).Build();
            var results = await CollectRowsAsync(scanner);
            Assert.Equal(expected, results);

            var scanTokenBuilder = _client.NewScanTokenBuilder(table)
                .LowerBound(lowerBound);
            var tokenResults = await CollectRowsAsync(scanTokenBuilder);
            Assert.Equal(expected, tokenResults);
        }

        { // Upper bound
            var maxRow = new Row("1", "3", "5");
            var upperBound = new PartialRow(schema);
            maxRow.FillPartialRow(upperBound);

            var expected = rows
                .Where(r => r.CompareTo(maxRow) < 0)
                .ToHashSet();

            var scanner = _client.NewScanBuilder(table)
                .ExclusiveUpperBound(upperBound).Build();
            var results = await CollectRowsAsync(scanner);
            Assert.Equal(expected, results);

            var scanTokenBuilder = _client.NewScanTokenBuilder(table)
                .ExclusiveUpperBound(upperBound);
            var tokenResults = await CollectRowsAsync(scanTokenBuilder);
            Assert.Equal(expected, tokenResults);
        }

        { // Lower & Upper bounds
            var minRow = new Row("1", "3", "5");
            var maxRow = new Row("2", "4", "");
            var lowerBound = new PartialRow(schema);
            minRow.FillPartialRow(lowerBound);
            var upperBound = new PartialRow(schema);
            maxRow.FillPartialRow(upperBound);

            var expected = rows
                .Where(r => r.CompareTo(minRow) >= 0 && r.CompareTo(maxRow) < 0)
                .ToHashSet();

            var scanner = _client.NewScanBuilder(table)
                .LowerBound(lowerBound)
                .ExclusiveUpperBound(upperBound)
                .Build();

            var results = await CollectRowsAsync(scanner);
            Assert.Equal(expected, results);

            var scanTokenBuilder = _client.NewScanTokenBuilder(table)
                .LowerBound(lowerBound)
                .ExclusiveUpperBound(upperBound);
            var tokenResults = await CollectRowsAsync(scanTokenBuilder);
            Assert.Equal(expected, tokenResults);
        }

        var tablets = await _client.GetTableLocationsAsync(
            table.TableId, null, 100);

        { // Per-tablet scan
            var results = new HashSet<Row>();

            foreach (var tablet in tablets)
            {
                var scanner = _client.NewScanBuilder(table)
                    .LowerBoundPartitionKeyRaw(tablet.Partition.PartitionKeyStart)
                    .ExclusiveUpperBoundPartitionKeyRaw(tablet.Partition.PartitionKeyEnd)
                    .Build();
                var tabletResults = await CollectRowsAsync(scanner);
                var intersection = results.Intersect(tabletResults);
                Assert.Empty(intersection);
                foreach (var row in tabletResults)
                    results.Add(row);
            }

            Assert.Equal(rows, results);
        }

        { // Per-tablet scan with lower & upper bounds
            var minRow = new Row("1", "3", "5");
            var maxRow = new Row("2", "4", "");
            var lowerBound = new PartialRow(schema);
            minRow.FillPartialRow(lowerBound);
            var upperBound = new PartialRow(schema);
            maxRow.FillPartialRow(upperBound);

            var expected = rows
                .Where(r => r.CompareTo(minRow) >= 0 && r.CompareTo(maxRow) < 0)
                .ToHashSet();
            var results = new HashSet<Row>();

            foreach (var tablet in tablets)
            {
                var scanner = _client.NewScanBuilder(table)
                    .LowerBound(lowerBound)
                    .ExclusiveUpperBound(upperBound)
                    .LowerBoundPartitionKeyRaw(tablet.Partition.PartitionKeyStart)
                    .ExclusiveUpperBoundPartitionKeyRaw(tablet.Partition.PartitionKeyEnd)
                    .Build();
                var tabletResults = await CollectRowsAsync(scanner);
                var intersection = results.Intersect(tabletResults);
                Assert.Empty(intersection);
                foreach (var row in tabletResults)
                    results.Add(row);
            }

            Assert.Equal(expected, results);
        }
    }

    private async Task InsertRowsAsync(KuduTable table, IEnumerable<Row> rows)
    {
        var operations = rows.Select(r =>
        {
            var insert = table.NewInsert();
            r.FillPartialRow(insert);
            return insert;
        });

        await _client.WriteAsync(operations);
    }

    private static async Task<HashSet<Row>> CollectRowsAsync(KuduScanner scanner)
    {
        var rows = new HashSet<Row>();
        await foreach (var resultSet in scanner)
        {
            foreach (var row in resultSet)
            {
                rows.Add(Row.FromResult(row));
            }
        }
        return rows;
    }

    private async Task<HashSet<Row>> CollectRowsAsync(KuduScanTokenBuilder builder)
    {
        var rows = new HashSet<Row>();
        var tokens = await builder.BuildAsync();

        foreach (var token in tokens)
        {
            var existingCount = rows.Count;
            var scanBuilder = await _client.NewScanBuilderFromTokenAsync(token);
            var scanner = scanBuilder.Build();

            var newRows = await CollectRowsAsync(scanner);
            rows.UnionWith(newRows);

            Assert.Equal(existingCount + newRows.Count, rows.Count);
        }

        return rows;
    }

    private static TableBuilder CreateTableBuilder(string tableName)
    {
        return new TableBuilder(tableName)
            .AddColumn("a", KuduType.String, opt => opt.Key(true))
            .AddColumn("b", KuduType.String, opt => opt.Key(true))
            .AddColumn("c", KuduType.String, opt => opt.Key(true));
    }

    private static HashSet<Row> CreateRows()
    {
        var rows = new HashSet<Row>();
        for (int a = 0; a < 6; a++)
        {
            for (int b = 0; b < 6; b++)
            {
                for (int c = 0; c < 6; c++)
                {
                    rows.Add(new Row(a.ToString(), b.ToString(), c.ToString()));
                }
            }
        }
        return rows;
    }

    private class Row : IEquatable<Row>, IComparable<Row>
    {
        public string ValA { get; }

        public string ValB { get; }

        public string ValC { get; }

        public Row(string valA, string valB, string valC)
        {
            ValA = valA;
            ValB = valB;
            ValC = valC;
        }

        public void FillPartialRow(PartialRow row)
        {
            row.SetString("a", ValA);
            row.SetString("b", ValB);
            row.SetString("c", ValC);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(ValA, ValB, ValC);
        }

        public override bool Equals(object obj) => Equals(obj as Row);

        public bool Equals(Row other)
        {
            if (other is null)
                return false;

            return
                ValA == other.ValA &&
                ValB == other.ValB &&
                ValC == other.ValC;
        }

        public int CompareTo(Row other)
        {
            int compareA = ValA.CompareTo(other.ValA);
            if (compareA != 0)
                return compareA;

            int compareB = ValB.CompareTo(other.ValB);
            if (compareB != 0)
                return compareB;

            int compareC = ValC.CompareTo(other.ValC);
            if (compareC != 0)
                return compareC;

            return 0;
        }

        public static Row FromResult(RowResult result)
        {
            return new Row(result.GetString("a"),
                           result.GetString("b"),
                           result.GetString("c"));
        }
    }
}
