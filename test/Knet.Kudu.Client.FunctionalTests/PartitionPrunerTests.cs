using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class PartitionPrunerTests : IAsyncLifetime
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
    public async Task TestPrimaryKeyRangePruning()
    {
        // CREATE TABLE t
        // (a INT8, b INT8, c INT8)
        // PRIMARY KEY (a, b, c))
        // PARTITION BY RANGE (a, b, c)
        //    (PARTITION                 VALUES < (0, 0, 0),
        //     PARTITION    (0, 0, 0) <= VALUES < (10, 10, 10)
        //     PARTITION (10, 10, 10) <= VALUES);

        var tableName = nameof(TestPrimaryKeyRangePruning);

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("a", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("b", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("c", KuduType.Int8, opt => opt.Key(true))
            .SetRangePartitionColumns("a", "b", "c")
            .AddSplitRow(row =>
            {
                row.SetByte("a", 0);
                row.SetByte("b", 0);
                row.SetByte("c", 0);
            })
            .AddSplitRow(row =>
            {
                row.SetByte("a", 10);
                row.SetByte("b", 10);
                row.SetByte("c", 10);
            });

        var table = await _client.CreateTableAsync(tableBuilder);
        var partitions = await GetTablePartitionsAsync(table);

        var min = sbyte.MinValue;

        // No bounds
        await CheckPartitionsPrimaryKeyAsync(3, table, partitions,
            null, null);

        // PK < (-1, min, min)
        await CheckPartitionsPrimaryKeyAsync(1, table, partitions,
            null, new sbyte[] { -1, min, min });

        // PK < (0, 0, 0)
        await CheckPartitionsPrimaryKeyAsync(1, table, partitions,
            null, new sbyte[] { 0, 0, 0 });

        // PK < (0, 0, min)
        await CheckPartitionsPrimaryKeyAsync(1, table, partitions,
            null, new sbyte[] { 0, 0, min });

        // PK < (10, 10, 10)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { 10, 10, 10 });

        // PK < (100, min, min)
        await CheckPartitionsPrimaryKeyAsync(3, table, partitions,
            null, new sbyte[] { 100, min, min });

        // PK >= (-10, -10, -10)
        await CheckPartitionsPrimaryKeyAsync(3, table, partitions,
            new sbyte[] { -10, -10, -10 }, null);

        // PK >= (0, 0, 0)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            new sbyte[] { 0, 0, 0 }, null);

        // PK >= (100, 0, 0)
        await CheckPartitionsPrimaryKeyAsync(1, table, partitions,
            new sbyte[] { 100, 0, 0 }, null);

        // PK >= (-10, 0, 0)
        // PK  < (100, 0, 0)
        await CheckPartitionsPrimaryKeyAsync(3, table, partitions,
            new sbyte[] { -10, 0, 0 }, new sbyte[] { 100, 0, 0 });

        // PK >= (0, 0, 0)
        // PK  < (10, 10, 10)
        await CheckPartitionsPrimaryKeyAsync(1, table, partitions,
            new sbyte[] { 0, 0, 0 }, new sbyte[] { 10, 0, 0 });

        // PK >= (0, 0, 0)
        // PK  < (10, 10, 11)
        await CheckPartitionsPrimaryKeyAsync(1, table, partitions,
            new sbyte[] { 0, 0, 0 }, new sbyte[] { 10, 0, 0 });

        // PK < (0, 0, 0)
        // PK >= (10, 10, 11)
        await CheckPartitionsPrimaryKeyAsync(0, table, partitions,
            new sbyte[] { 10, 0, 0 }, new sbyte[] { 0, 0, 0 });
    }

    [SkippableFact]
    public async Task TestPrimaryKeyPrefixRangePruning()
    {
        // CREATE TABLE t
        // (a INT8, b INT8, c INT8)
        // PRIMARY KEY (a, b, c))
        // PARTITION BY RANGE (a, b)
        //    (PARTITION VALUES < (0, 0, 0));

        var tableName = nameof(TestPrimaryKeyPrefixRangePruning);

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("a", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("b", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("c", KuduType.Int8, opt => opt.Key(true))
            .SetRangePartitionColumns("a", "b")
            .AddSplitRow(row =>
            {
                row.SetByte("a", 0);
                row.SetByte("b", 0);
            });

        var table = await _client.CreateTableAsync(tableBuilder);
        var partitions = await GetTablePartitionsAsync(table);

        var min = sbyte.MinValue;
        var max = sbyte.MaxValue;

        // No bounds
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
                                  null, null);

        // PK < (-1, min, min)
        // TODO(KUDU-2178): prune the upper partition.
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { -1, min, min });

        // PK < (0, 0, min)
        // TODO(KUDU-2178): prune the upper partition.
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { 0, 0, min });

        // PK < (0, 0, 0)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { 0, 0, 0 });

        // PK < (0, 1, min)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { 0, 1, min });

        // PK < (0, 1, 0)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { 0, 1, 0 });

        // PK < (max, max, min)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { max, max, min });

        // PK < (max, max, 0)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            null, new sbyte[] { max, max, 0 });

        // PK >= (0, 0, min)
        // TODO(KUDU-2178): prune the lower partition.
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            new sbyte[] { 0, 0, min }, null);

        // PK >= (0, 0, 0)
        // TODO(KUDU-2178): prune the lower partition.
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            new sbyte[] { 0, 0, 0 }, null);

        // PK >= (0, -1, 0)
        await CheckPartitionsPrimaryKeyAsync(2, table, partitions,
            new sbyte[] { 0, -1, 0 }, null);
    }

    [SkippableFact]
    public async Task TestRangePartitionPruning()
    {
        // CREATE TABLE t
        // (a INT8, b STRING, c INT8)
        // PRIMARY KEY (a, b, c))
        // PARTITION BY RANGE (c, b)
        //    (PARTITION              VALUES < (0, "m"),
        //     PARTITION  (0, "m") <= VALUES < (10, "r")
        //     PARTITION (10, "r") <= VALUES);

        var tableName = nameof(TestRangePartitionPruning);

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("a", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("b", KuduType.String, opt => opt.Key(true))
            .AddColumn("c", KuduType.Int8, opt => opt.Key(true))
            .SetRangePartitionColumns("c", "b")
            .AddSplitRow(row =>
            {
                row.SetByte("c", 0);
                row.SetString("b", "m");
            })
            .AddSplitRow(row =>
            {
                row.SetByte("c", 10);
                row.SetString("b", "r");
            });

        var table = await _client.CreateTableAsync(tableBuilder);
        var partitions = await GetTablePartitionsAsync(table);

        var b = table.Schema.GetColumn("b");
        var c = table.Schema.GetColumn("c");

        // No Predicates
        await CheckPartitionsAsync(3, 1, table, partitions);

        // c < -10
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, -10));

        // c = -10
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, -10));

        // c < 10
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, 10));

        // c < 100
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, 100));

        // c < MIN
        await CheckPartitionsAsync(0, 0, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, sbyte.MinValue));

        // c < MAX
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, sbyte.MaxValue));

        // c >= -10
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, -10));

        // c >= 0
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, -10));

        // c >= 5
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, 5));

        // c >= 10
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, 10));

        // c >= 100
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, 100));

        // c >= MIN
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, sbyte.MinValue));

        // c >= MAX
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, sbyte.MaxValue));

        // c >= -10
        // c < 0
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, -10),
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, 0));

        // c >= 5
        // c < 100
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, 5),
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, 100));

        // b = ""
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Equal, ""));

        // b >= "z"
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.GreaterEqual, "z"));

        // b < "a"
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "a"));

        // b >= "m"
        // b < "z"
        await CheckPartitionsAsync(3, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.GreaterEqual, "m"),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "z"));

        // c >= 10
        // b >= "r"
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, 10),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.GreaterEqual, "r"));

        // c >= 10
        // b < "r"
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.GreaterEqual, 10),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "r"));

        // c = 10
        // b < "r"
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 10),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "r"));

        // c < 0
        // b < "m"
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 0),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "m"));

        // c < 0
        // b < "z"
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Less, 0),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "z"));

        // c = 0
        // b = "m\0"
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 0),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Equal, "m\0"));

        // c = 0
        // b < "m"
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 0),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "m"));

        // c = 0
        // b < "m\0"
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 0),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "m\0"));

        // c = 0
        // c = 2
        await CheckPartitionsAsync(0, 0, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 0),
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 2));

        // c = MIN
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, sbyte.MinValue));

        // c = MAX
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, sbyte.MaxValue));

        // c IN (1, 2)
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewInListPredicate(c, new byte[] { 1, 2 }));

        // c IN (0, 1, 2)
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1, 2 }));

        // c IN (-10, 0)
        // b < "m"
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewInListPredicate(c, new sbyte[] { -10, 0 }),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "m"));

        // c IN (-10, 0)
        // b < "m\0"
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewInListPredicate(c, new sbyte[] { -10, 0 }),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Less, "m\0"));
    }

    [SkippableFact]
    public async Task TestHashPartitionPruning()
    {
        // CREATE TABLE t
        // (a INT8, b INT8, c INT8)
        // PRIMARY KEY (a, b, c)
        // PARTITION BY HASH (a) PARTITIONS 2,
        //              HASH (b, c) PARTITIONS 2;

        var tableName = nameof(TestHashPartitionPruning);

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("a", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("b", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("c", KuduType.Int8, opt => opt.Key(true))
            .AddHashPartitions(2, "a")
            .AddHashPartitions(2, "b", "c");

        var table = await _client.CreateTableAsync(tableBuilder);
        var partitions = await GetTablePartitionsAsync(table);

        var a = table.Schema.GetColumn("a");
        var b = table.Schema.GetColumn("b");
        var c = table.Schema.GetColumn("c");

        // No Predicates
        await CheckPartitionsAsync(4, 1, table, partitions);

        // a = 0;
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(a, ComparisonOp.Equal, 0));

        // a >= 0;
        await CheckPartitionsAsync(4, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(a, ComparisonOp.GreaterEqual, 0));

        // a >= 0;
        // a < 1;
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(a, ComparisonOp.GreaterEqual, 0),
            KuduPredicate.NewComparisonPredicate(a, ComparisonOp.Less, 1));

        // a >= 0;
        // a < 2;
        await CheckPartitionsAsync(4, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(a, ComparisonOp.GreaterEqual, 0),
            KuduPredicate.NewComparisonPredicate(a, ComparisonOp.Less, 2));

        // b = 1;
        await CheckPartitionsAsync(4, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Equal, 1));

        // b = 1;
        // c = 2;
        await CheckPartitionsAsync(2, 2, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Equal, 1),
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 2));

        // a = 0;
        // b = 1;
        // c = 2;
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(a, ComparisonOp.Equal, 0),
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Equal, 1),
            KuduPredicate.NewComparisonPredicate(c, ComparisonOp.Equal, 2));

        // a IN (0, 10)
        await CheckPartitionsAsync(4, 1, table, partitions,
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 10 }));
    }

    [SkippableFact]
    public async Task TestInListHashPartitionPruning()
    {
        // CREATE TABLE t
        // (a INT8, b INT8, c INT8)
        // PRIMARY KEY (a, b, c)
        // PARTITION BY HASH (a) PARTITIONS 3,
        //              HASH (b) PARTITIONS 3,
        //              HASH (c) PARTITIONS 3;

        var tableName = nameof(TestInListHashPartitionPruning);

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("a", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("b", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("c", KuduType.Int8, opt => opt.Key(true))
            .AddHashPartitions(3, "a")
            .AddHashPartitions(3, "b")
            .AddHashPartitions(3, "c");

        var table = await _client.CreateTableAsync(tableBuilder);
        var partitions = await GetTablePartitionsAsync(table);

        var a = table.Schema.GetColumn("a");
        var b = table.Schema.GetColumn("b");
        var c = table.Schema.GetColumn("c");

        // a in [0, 1];
        await CheckPartitionsAsync(18, 2, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1 }));

        // a in [0, 1, 8];
        await CheckPartitionsAsync(27, 1, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1, 8 }));

        // b in [0, 1];
        await CheckPartitionsAsync(18, 6, table, partitions,
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1 }));

        // c in [0, 1];
        await CheckPartitionsAsync(18, 18, table, partitions,
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));

        // b in [0, 1], c in [0, 1];
        await CheckPartitionsAsync(12, 12, table, partitions,
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1 }),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));

        // a in [0, 1], b in [0, 1], c in [0, 1];
        await CheckPartitionsAsync(8, 8, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1 }),
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1 }),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));

        // a in [0, 1, 2], b in [0, 1, 2], c in [0, 1, 2];
        await CheckPartitionsAsync(8, 8, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1, 2 }),
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1, 2 }),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1, 2 }));

        // a in [0, 1, 2, 3], b in [0, 1, 2, 3], c in [0, 1, 2, 3];
        await CheckPartitionsAsync(8, 8, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1, 2, 3 }),
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1, 2, 3 }),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1, 2, 3 }));

        var expectedList = new List<List<int>>
        {
            new List<int> { 1, 1 },
            new List<int> { 8, 8 },
            new List<int> { 8, 8 },
            new List<int> { 8, 8 },
            new List<int> { 27, 1 },
            new List<int> { 27, 1 },
            new List<int> { 27, 1 },
            new List<int> { 27, 1 },
            new List<int> { 27, 1 },
            new List<int> { 27, 1 }
        };

        for (int size = 1; size <= 10; size++)
        {
            int columnCount = table.Schema.Columns.Count;
            var testCases = new List<List<byte>>();

            for (int i = 0; i < columnCount; i++)
            {
                var testCase = new List<byte>();
                for (int j = 0; j < size; j++)
                {
                    testCase.Add((byte)j);
                }

                testCases.Add(testCase);
            }

            var scanBuilder = _client.NewScanBuilder(table);

            var columnSchemas = new List<ColumnSchema> { a, b, c };
            var predicates = new KuduPredicate[3];

            for (int i = 0; i < 3; i++)
            {
                predicates[i] = KuduPredicate.NewInListPredicate(
                    columnSchemas[i], testCases[i]);

                scanBuilder.AddPredicate(predicates[i]);
            }

            await CheckPartitionsAsync(
                expectedList[size - 1][0],
                expectedList[size - 1][1],
                table, partitions, predicates);

            var partitionSchema = scanBuilder.Table.PartitionSchema;
            Assert.Equal(columnCount, partitionSchema.HashBucketSchemas.Count);
        }
    }

    [SkippableFact]
    public async Task TestMultiColumnInListHashPruning()
    {
        // CREATE TABLE t
        // (a INT8, b INT8, c INT8)
        // PRIMARY KEY (a, b, c)
        // PARTITION BY HASH (a) PARTITIONS 3,
        //              HASH (b, c) PARTITIONS 3;

        var tableName = nameof(TestMultiColumnInListHashPruning);

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("a", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("b", KuduType.Int8, opt => opt.Key(true))
            .AddColumn("c", KuduType.Int8, opt => opt.Key(true))
            .AddHashPartitions(3, "a")
            .AddHashPartitions(3, "b", "c");

        var table = await _client.CreateTableAsync(tableBuilder);
        var partitions = await GetTablePartitionsAsync(table);

        var a = table.Schema.GetColumn("a");
        var b = table.Schema.GetColumn("b");
        var c = table.Schema.GetColumn("c");

        // a in [0, 1];
        await CheckPartitionsAsync(6, 2, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1 }));

        // a in [0, 1, 8];
        await CheckPartitionsAsync(9, 1, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1, 8 }));

        // b in [0, 1];
        await CheckPartitionsAsync(9, 1, table, partitions,
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1 }));

        // c in [0, 1];
        await CheckPartitionsAsync(9, 1, table, partitions,
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));

        // b in [0, 1], c in [0, 1]
        // (0, 0) in bucket 2
        // (0, 1) in bucket 2
        // (1, 0) in bucket 1
        // (1, 1) in bucket 0
        await CheckPartitionsAsync(9, 1, table, partitions,
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1 }),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));

        // b = 0, c in [0, 1]
        await CheckPartitionsAsync(3, 3, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Equal, 0),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));

        // b = 1, c in [0, 1]
        await CheckPartitionsAsync(6, 6, table, partitions,
            KuduPredicate.NewComparisonPredicate(b, ComparisonOp.Equal, 1),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));

        // a in [0, 1], b in [0, 1], c in [0, 1];
        await CheckPartitionsAsync(6, 2, table, partitions,
            KuduPredicate.NewInListPredicate(a, new byte[] { 0, 1 }),
            KuduPredicate.NewInListPredicate(b, new byte[] { 0, 1 }),
            KuduPredicate.NewInListPredicate(c, new byte[] { 0, 1 }));
    }

    [SkippableFact]
    public async Task TestPruning()
    {
        // CREATE TABLE timeseries
        // (host STRING, metric STRING, timestamp UNIXTIME_MICROS, value DOUBLE)
        // PRIMARY KEY (host, metric, time)
        // DISTRIBUTE BY
        //    RANGE(time)
        //        (PARTITION VALUES < 10,
        //         PARTITION VALUES >= 10);
        //    HASH (host, metric) 2 PARTITIONS;

        var tableName = nameof(TestPruning);

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("host", KuduType.String, opt => opt.Key(true))
            .AddColumn("metric", KuduType.String, opt => opt.Key(true))
            .AddColumn("timestamp", KuduType.UnixtimeMicros, opt => opt.Key(true))
            .AddColumn("value", KuduType.Double)
            .SetRangePartitionColumns("timestamp")
            .AddSplitRow(row => row.SetInt64("timestamp", 10))
            .AddHashPartitions(2, "host", "metric");

        var table = await _client.CreateTableAsync(tableBuilder);
        var partitions = await GetTablePartitionsAsync(table);

        var host = table.Schema.GetColumn("host");
        var metric = table.Schema.GetColumn("metric");
        var timestamp = table.Schema.GetColumn("timestamp");

        // No Predicates
        await CheckPartitionsAsync(4, 1, table, partitions);

        // host = "a"
        await CheckPartitionsAsync(4, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"));

        // host = "a"
        // metric = "a"
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(metric, ComparisonOp.Equal, "a"));

        // host = "a"
        // metric = "a"
        // timestamp >= 9;
        await CheckPartitionsAsync(2, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(metric, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.GreaterEqual, 9));

        // host = "a"
        // metric = "a"
        // timestamp >= 10;
        // timestamp < 20;
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(metric, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.GreaterEqual, 10),
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.Less, 20));

        // host = "a"
        // metric = "a"
        // timestamp < 10;
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(metric, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.Less, 10));

        // host = "a"
        // metric = "a"
        // timestamp >= 10;
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(metric, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.GreaterEqual, 10));

        // host = "a"
        // metric = "a"
        // timestamp = 10;
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(metric, ComparisonOp.Equal, "a"),
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.Equal, 10));

        byte[] hash1 = new byte[] { 0, 0, 0, 1 };

        // partition key < (hash=1)
        await CheckPartitionsAsync(2, 1, table, partitions, null, hash1);

        // partition key >= (hash=1)
        await CheckPartitionsAsync(2, 1, table, partitions, hash1, null);

        // timestamp = 10
        // partition key < (hash=1)
        await CheckPartitionsAsync(1, 1, table, partitions, null, hash1,
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.Equal, 10));

        // timestamp = 10
        // partition key >= (hash=1)
        await CheckPartitionsAsync(1, 1, table, partitions, hash1, null,
            KuduPredicate.NewComparisonPredicate(timestamp, ComparisonOp.Equal, 10));

        // timestamp IN (0, 9)
        // host = "a"
        // metric IN ("foo", "baz")
        await CheckPartitionsAsync(1, 1, table, partitions,
            KuduPredicate.NewInListPredicate(timestamp, new long[] { 0, 9 }),
            KuduPredicate.NewComparisonPredicate(host, ComparisonOp.Equal, "a"),
            KuduPredicate.NewInListPredicate(metric, new string[] { "foo", "baz" }));

        // timestamp IN (10, 100)
        await CheckPartitionsAsync(2, 2, table, partitions,
            KuduPredicate.NewInListPredicate(timestamp, new long[] { 10, 100 }));

        // timestamp IN (9, 10)
        await CheckPartitionsAsync(4, 2, table, partitions,
            KuduPredicate.NewInListPredicate(timestamp, new long[] { 9, 10 }));

        // timestamp IS NOT NULL
        await CheckPartitionsAsync(4, 1, table, partitions,
            KuduPredicate.NewIsNotNullPredicate(timestamp));

        // timestamp IS NULL
        await CheckPartitionsAsync(0, 0, table, partitions,
            KuduPredicate.NewIsNullPredicate(timestamp));
    }

    /// <summary>
    /// Counts the partitions touched by a scan with optional primary key bounds.
    /// The table is assumed to have three INT8 columns as the primary key.
    /// </summary>
    /// <param name="expectedTablets">The expected number of tablets to satisfy the scan.</param>
    /// <param name="table">The table to scan.</param>
    /// <param name="partitions">The partitions of the table.</param>
    /// <param name="lowerBoundPrimaryKey">The optional lower bound primary key.</param>
    /// <param name="upperBoundPrimaryKey">The optional upper bound primary key.</param>
    private async Task CheckPartitionsPrimaryKeyAsync(
        int expectedTablets,
        KuduTable table,
        List<Partition> partitions,
        sbyte[] lowerBoundPrimaryKey,
        sbyte[] upperBoundPrimaryKey)
    {
        var scanBuilder = _client.NewScanTokenBuilder(table);

        if (lowerBoundPrimaryKey != null)
        {
            var lower = new PartialRow(table.Schema);
            for (int i = 0; i < 3; i++)
            {
                lower.SetSByte(i, lowerBoundPrimaryKey[i]);
            }
            scanBuilder.LowerBound(lower);
        }

        if (upperBoundPrimaryKey != null)
        {
            var upper = new PartialRow(table.Schema);
            for (int i = 0; i < 3; i++)
            {
                upper.SetSByte(i, upperBoundPrimaryKey[i]);
            }
            scanBuilder.ExclusiveUpperBound(upper);
        }

        var pruner = PartitionPruner.Create(scanBuilder);

        int scannedPartitions = 0;
        foreach (var partition in partitions)
        {
            if (!pruner.ShouldPruneForTests(partition))
            {
                scannedPartitions++;
            }
        }

        // Check that the number of ScanTokens built for the scan matches.
        var tokens = await scanBuilder.BuildAsync();
        Assert.Equal(expectedTablets, scannedPartitions);
        Assert.Equal(scannedPartitions, tokens.Count);
        Assert.Equal(expectedTablets == 0 ? 0 : 1, pruner.NumRangesRemaining);
    }

    /// <summary>
    /// Checks the number of tablets and pruner ranges generated for a scan.
    /// </summary>
    /// <param name="expectedTablets">The expected number of tablets to satisfy the scan.</param>
    /// <param name="expectedPrunerRanges">The expected number of generated partition pruner ranges.</param>
    /// <param name="table">The table to scan.</param>
    /// <param name="partitions">The partitions of the table.</param>
    /// <param name="predicates">The predicates to apply to the scan.</param>
    private Task CheckPartitionsAsync(
        int expectedTablets,
        int expectedPrunerRanges,
        KuduTable table,
        List<Partition> partitions,
        params KuduPredicate[] predicates)
    {
        return CheckPartitionsAsync(
            expectedTablets,
            expectedPrunerRanges,
            table,
            partitions,
            null,
            null,
            predicates);
    }

    /// <summary>
    /// Checks the number of tablets and pruner ranges generated for a scan with
    /// predicates and optional partition key bounds.
    /// </summary>
    /// <param name="expectedTablets">The expected number of tablets to satisfy the scan.</param>
    /// <param name="expectedPrunerRanges">The expected number of generated partition pruner ranges.</param>
    /// <param name="table">The table to scan.</param>
    /// <param name="partitions">The partitions of the table.</param>
    /// <param name="lowerBoundPartitionKey">An optional lower bound partition key.</param>
    /// <param name="upperBoundPartitionKey">An optional upper bound partition key.</param>
    /// <param name="predicates">The predicates to apply to the scan.</param>
    private async Task CheckPartitionsAsync(
        int expectedTablets,
        int expectedPrunerRanges,
        KuduTable table,
        List<Partition> partitions,
        byte[] lowerBoundPartitionKey,
        byte[] upperBoundPartitionKey,
        params KuduPredicate[] predicates)
    {
        // Partition key bounds can't be applied to the ScanTokenBuilder.
        var scanBuilder = _client.NewScanBuilder(table);

        foreach (var predicate in predicates)
            scanBuilder.AddPredicate(predicate);

        if (lowerBoundPartitionKey != null)
            scanBuilder.LowerBoundPartitionKeyRaw(lowerBoundPartitionKey);

        if (upperBoundPartitionKey != null)
            scanBuilder.ExclusiveUpperBoundPartitionKeyRaw(upperBoundPartitionKey);

        var pruner = PartitionPruner.Create(scanBuilder);

        int scannedPartitions = 0;
        foreach (var partition in partitions)
        {
            if (!pruner.ShouldPruneForTests(partition))
            {
                scannedPartitions++;
            }
        }

        Assert.Equal(expectedTablets, scannedPartitions);
        Assert.Equal(expectedPrunerRanges, pruner.NumRangesRemaining);

        // Check that the scan token builder comes up with the same amount.
        // The scan token builder does not allow for upper/lower partition keys.
        if (lowerBoundPartitionKey == null && upperBoundPartitionKey == null)
        {
            var tokenBuilder = _client.NewScanTokenBuilder(table);

            foreach (var predicate in predicates)
                tokenBuilder.AddPredicate(predicate);

            // Check that the number of ScanTokens built for the scan matches.
            var tokens = await tokenBuilder.BuildAsync();
            Assert.Equal(expectedTablets, tokens.Count);
        }
    }

    /// <summary>
    /// Retrieves the partitions of a table.
    /// </summary>
    private async Task<List<Partition>> GetTablePartitionsAsync(KuduTable table)
    {
        var tokens = await _client.NewScanTokenBuilder(table).BuildAsync();
        return tokens.Select(token => token.Tablet.Partition).ToList();
    }
}
