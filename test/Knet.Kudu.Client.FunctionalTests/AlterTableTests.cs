using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class AlterTableTests : IAsyncLifetime
{
    private readonly string _tableName = "AlterTableTests-table";
    private KuduTestHarness _harness;
    private KuduClient _client;
    private IKuduSession _session;

    public async Task InitializeAsync()
    {
        _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
        _client = _harness.CreateClient();
        _session = _client.NewSession();
    }

    public async Task DisposeAsync()
    {
        await _session.DisposeAsync();
        await _client.DisposeAsync();
        await _harness.DisposeAsync();
    }

    [SkippableFact]
    public async Task TestAlterAddColumns()
    {
        KuduTable table = await CreateTableAsync();
        await InsertRowsAsync(table, 0, 100);
        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .AddColumn("addNonNull", KuduType.Int32, opt => opt
                .Nullable(false)
                .DefaultValue(100))
            .AddColumn("addNullable", KuduType.Int32)
            .AddColumn("addNullableDef", KuduType.Int32, opt => opt
                .DefaultValue(200)));

        // Reopen table for the new schema.
        table = await _client.OpenTableAsync(_tableName);
        Assert.Equal(5, table.Schema.Columns.Count);

        // Add a row with addNullableDef=null
        var insert = table.NewInsert();
        insert.SetInt32("c0", 101);
        insert.SetInt32("c1", 101);
        insert.SetInt32("addNonNull", 101);
        insert.SetInt32("addNullable", 101);
        insert.SetNull("addNullableDef");
        await _session.EnqueueAsync(insert);
        await _session.FlushAsync();

        // Check defaults applied, and that row key=101
        var results = await ClientTestUtil.ScanTableToStringsAsync(_client, table);

        var expected = new List<string>(101);
        for (int i = 0; i < 100; i++)
        {
            expected.Add($"INT32 c0={i}, INT32 c1={i}, INT32 addNonNull=100, " +
                          "INT32 addNullable=NULL, INT32 addNullableDef=200");
        }

        expected.Add("INT32 c0=101, INT32 c1=101, INT32 addNonNull=101, " +
                     "INT32 addNullable=101, INT32 addNullableDef=NULL");

        Assert.Equal(
            expected.OrderBy(r => r),
            results.OrderBy(r => r));
    }

    [SkippableFact]
    public async Task TestAlterModifyColumns()
    {
        KuduTable table = await CreateTableAsync();
        await InsertRowsAsync(table, 0, 100);
        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // Check for expected defaults.
        ColumnSchema col = table.Schema.GetColumn(1);
        Assert.Equal(CompressionType.DefaultCompression, col.Compression);
        Assert.Equal(EncodingType.AutoEncoding, col.Encoding);
        Assert.Null(col.DefaultValue);

        // Alter the table.
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .ChangeCompressionAlgorithm(col.Name, CompressionType.Snappy)
            .ChangeEncoding(col.Name, EncodingType.Rle)
            .ChangeDefault(col.Name, 0));

        // Check for new values.
        table = await _client.OpenTableAsync(_tableName);
        col = table.Schema.GetColumn(1);
        Assert.Equal(CompressionType.Snappy, col.Compression);
        Assert.Equal(EncodingType.Rle, col.Encoding);
        Assert.Equal(0, col.DefaultValue);
    }

    [SkippableFact]
    public async Task TestRenameKeyColumn()
    {
        KuduTable table = await CreateTableAsync();
        await InsertRowsAsync(table, 0, 100);
        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .RenameColumn("c0", "c0Key"));

        var exception = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            // Scanning with the old schema.
            var scanner = _client.NewScanBuilder(table)
                .SetProjectedColumns("c0", "c1")
                .Build();

            await foreach (var resultSet in scanner) { }
        });

        Assert.True(exception.Status.IsInvalidArgument);
        Assert.Contains(
            "Some columns are not present in the current schema: c0",
            exception.Status.Message);

        // Reopen table for the new schema.
        table = await _client.OpenTableAsync(_tableName);
        Assert.Equal("c0Key", table.Schema.GetColumn(0).Name);
        Assert.Equal(2, table.Schema.Columns.Count);

        // Add a row
        var insert = table.NewInsert();
        insert.SetInt32("c0Key", 101);
        insert.SetInt32("c1", 101);
        await _session.EnqueueAsync(insert);
        await _session.FlushAsync();

        var scanner2 = _client.NewScanBuilder(table)
            .SetProjectedColumns("c0Key", "c1")
            .Build();

        var rows = await scanner2.ScanToListAsync<(int c0Key, int c1)>();
        Assert.Equal(101, rows.Count);
        Assert.All(rows, row => Assert.Equal(row.c0Key, row.c1));
    }

    [SkippableFact]
    public async Task TestAlterRangePartitioning()
    {
        KuduTable table = await CreateTableAsync();
        KuduSchema schema = table.Schema;

        // Insert some rows, and then drop the partition and ensure that the table is empty.
        await InsertRowsAsync(table, 0, 100);
        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .DropRangePartition((lower, upper) => { }));
        Assert.Equal(0, await ClientTestUtil.CountRowsAsync(_client, table));

        // Add new range partition and insert rows.
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 0);
                upper.SetInt32("c0", 100);
            }));
        await InsertRowsAsync(table, 0, 100);
        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // Replace the range partition with a different one.
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .DropRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 0);
                upper.SetInt32("c0", 100);
            })
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 50);
                upper.SetInt32("c0", 150);
            }));

        Assert.Equal(0, await ClientTestUtil.CountRowsAsync(_client, table));
        await InsertRowsAsync(table, 50, 125);
        Assert.Equal(75, await ClientTestUtil.CountRowsAsync(_client, table));

        // Replace the range partition with the same one.
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .DropRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 50);
                upper.SetInt32("c0", 150);
            })
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 50);
                upper.SetInt32("c0", 150);
            }));

        Assert.Equal(0, await ClientTestUtil.CountRowsAsync(_client, table));
        await InsertRowsAsync(table, 50, 125);
        Assert.Equal(75, await ClientTestUtil.CountRowsAsync(_client, table));

        // Alter table partitioning + alter table schema
        var newTableName = $"{_tableName}-renamed";
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 200);
                upper.SetInt32("c0", 300);
            })
            .RenameTable(newTableName)
            .AddColumn("c2", KuduType.Int32));

        await InsertRowsAsync(table, 200, 300);
        Assert.Equal(175, await ClientTestUtil.CountRowsAsync(_client, table));
        Assert.Equal(3, (await _client.OpenTableAsync(newTableName)).Schema.Columns.Count);

        // Drop all range partitions + alter table schema. This also serves to test
        // specifying range bounds with a subset schema (since a column was
        // previously added).
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .DropRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 200);
                upper.SetInt32("c0", 300);
            })
            .DropRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 50);
                upper.SetInt32("c0", 150);
            })
            .DropColumn("c2"));

        Assert.Equal(0, await ClientTestUtil.CountRowsAsync(_client, table));
        Assert.Equal(2, (await _client.OpenTableAsync(newTableName)).Schema.Columns.Count);
    }

    [SkippableFact]
    public async Task TestAlterRangePartitioningExclusiveInclusive()
    {
        // Create initial table with single range partition covering (-1, 99].
        var builder = new TableBuilder(_tableName)
            .SetNumReplicas(1)
            .AddColumn("c0", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("c1", KuduType.Int32, opt => opt.Nullable(false))
            .SetRangePartitionColumns("c0")
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", -1);
                upper.SetInt32("c0", 99);
            }, RangePartitionBound.Exclusive, RangePartitionBound.Inclusive);

        KuduTable table = await _client.CreateTableAsync(builder);

        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 199);
                upper.SetInt32("c0", 299);
            }, RangePartitionBound.Exclusive, RangePartitionBound.Inclusive));

        // Insert some rows, and then drop the partition and ensure that the table is empty.
        await InsertRowsAsync(table, 0, 100);
        await InsertRowsAsync(table, 200, 300);
        Assert.Equal(200, await ClientTestUtil.CountRowsAsync(_client, table));

        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .DropRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 0);
                upper.SetInt32("c0", 100);
            }, RangePartitionBound.Inclusive, RangePartitionBound.Exclusive)
            .DropRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", 199);
                upper.SetInt32("c0", 299);
            }, RangePartitionBound.Exclusive, RangePartitionBound.Inclusive));

        Assert.Equal(0, await ClientTestUtil.CountRowsAsync(_client, table));
    }

    [SkippableFact]
    public async Task TestAlterRangeParitioningInvalid()
    {
        // Create initial table with single range partition covering [0, 100).
        KuduTable table = await CreateTableAsync((0, 100));
        await InsertRowsAsync(table, 0, 100);
        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // ADD [0, 100) <- already present (duplicate)
        var ex = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 0);
                    upper.SetInt32("c0", 100);
                }));
        });

        Assert.True(ex.Status.IsAlreadyPresent);
        Assert.Contains("range partition already exists", ex.Status.Message);

        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // ADD [50, 150) <- illegal (overlap)
        ex = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 50);
                    upper.SetInt32("c0", 150);
                }));
        });

        Assert.True(ex.Status.IsInvalidArgument);
        Assert.Contains("new range partition conflicts with existing one", ex.Status.Message);

        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // ADD [-50, 50) <- illegal (overlap)
        ex = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", -50);
                    upper.SetInt32("c0", 50);
                }));
        });

        Assert.True(ex.Status.IsInvalidArgument);
        Assert.Contains("new range partition conflicts with existing one", ex.Status.Message);

        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // ADD [200, 300)
        // ADD [-50, 150) <- illegal (overlap)
        ex = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 200);
                    upper.SetInt32("c0", 300);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", -50);
                    upper.SetInt32("c0", 150);
                }));
        });

        Assert.True(ex.Status.IsInvalidArgument);
        Assert.Contains("new range partition conflicts with existing one", ex.Status.Message);

        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // DROP [<start>, <end>)
        ex = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .DropRangePartition((lower, upper) => { }));
        });

        Assert.True(ex.Status.IsInvalidArgument);
        Assert.Contains("no range partition to drop", ex.Status.Message);

        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // DROP [50, 150)
        // RENAME foo
        ex = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .DropRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 50);
                    upper.SetInt32("c0", 150);
                })
                .RenameTable("foo"));
        });

        Assert.True(ex.Status.IsInvalidArgument);
        Assert.Contains("no range partition to drop", ex.Status.Message);

        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));
        Assert.Empty(await _client.GetTablesAsync("foo"));

        // DROP [0, 100)
        // ADD  [100, 200)
        // DROP [100, 200)
        // ADD  [150, 250)
        // DROP [0, 10)    <- illegal
        ex = await Assert.ThrowsAsync<NonRecoverableException>(async () =>
        {
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .DropRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 0);
                    upper.SetInt32("c0", 100);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 100);
                    upper.SetInt32("c0", 200);
                })
                .DropRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 100);
                    upper.SetInt32("c0", 200);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 150);
                    upper.SetInt32("c0", 250);
                })
                .DropRangePartition((lower, upper) =>
                {
                    lower.SetInt32("c0", 0);
                    upper.SetInt32("c0", 10);
                }));
        });

        Assert.True(ex.Status.IsInvalidArgument);
        Assert.Contains("no range partition to drop", ex.Status.Message);

        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));
    }

    [SkippableFact]
    public async Task TestAlterExtraConfigs()
    {
        KuduTable table = await CreateTableAsync();
        await InsertRowsAsync(table, 0, 100);
        Assert.Equal(100, await ClientTestUtil.CountRowsAsync(_client, table));

        // 1. Check for expected defaults.
        table = await _client.OpenTableAsync(_tableName);
        Assert.DoesNotContain("kudu.table.history_max_age_sec", table.ExtraConfig);

        // 2. Alter history max age second to 3600
        var alterExtraConfigs = new Dictionary<string, string>
            {
                { "kudu.table.history_max_age_sec", "3600" }
            };
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .AlterExtraConfigs(alterExtraConfigs));

        table = await _client.OpenTableAsync(_tableName);
        Assert.Equal("3600", table.ExtraConfig["kudu.table.history_max_age_sec"]);

        // 3. Alter history max age second to 7200
        alterExtraConfigs = new Dictionary<string, string>
            {
                { "kudu.table.history_max_age_sec", "7200" }
            };
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .AlterExtraConfigs(alterExtraConfigs));

        table = await _client.OpenTableAsync(_tableName);
        Assert.Equal("7200", table.ExtraConfig["kudu.table.history_max_age_sec"]);

        // 4. Reset history max age second to default
        alterExtraConfigs = new Dictionary<string, string>
            {
                { "kudu.table.history_max_age_sec", "" }
            };
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .AlterExtraConfigs(alterExtraConfigs));

        table = await _client.OpenTableAsync(_tableName);
        Assert.Empty(table.ExtraConfig);
    }

    [SkippableFact]
    public async Task TestAlterChangeOwner()
    {
        var originalOwner = "alice";
        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestAlterChangeOwner))
            .SetRangePartitionColumns("key")
            .SetOwner(originalOwner);

        var table = await _client.CreateTableAsync(builder);
        Assert.Equal(originalOwner, table.Owner);

        var newOwner = "bob";
        await _client.AlterTableAsync(new AlterTableBuilder(table)
            .SetOwner(newOwner));
        table = await _client.OpenTableAsync(table.TableName);
        Assert.Equal(newOwner, table.Owner);
    }

    [SkippableFact]
    public async Task TestAlterChangeComment()
    {
        var originalComment = "original comment";
        var builder = new TableBuilder(nameof(TestAlterAddColumns))
            .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("val", KuduType.Int32)
            .SetRangePartitionColumns("key")
            .SetComment(originalComment);

        var table = await _client.CreateTableAsync(builder);
        Assert.Equal(originalComment, table.Comment);

        var newComment = "new comment";
        await _client.AlterTableAsync(new AlterTableBuilder(table).SetComment(newComment));
        table = await _client.OpenTableAsync(table.TableName);
        Assert.Equal(newComment, table.Comment);
    }

    /// <summary>
    /// Creates a new table with two int columns, c0 and c1. c0 is the primary key.
    /// The table is hash partitioned on c0 into two buckets, and range partitioned
    /// with the provided bounds.
    /// </summary>
    private async Task<KuduTable> CreateTableAsync(params (int lower, int upper)[] bounds)
    {
        // Create initial table with single range partition covering the entire key
        // space, and two hash buckets.
        var builder = new TableBuilder(_tableName)
            .SetNumReplicas(1)
            .AddColumn("c0", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("c1", KuduType.Int32, opt => opt.Nullable(false))
            .AddHashPartitions(2, "c0")
            .SetRangePartitionColumns("c0");

        foreach (var (lowerBound, upperBound) in bounds)
        {
            builder.AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("c0", lowerBound);
                upper.SetInt32("c0", upperBound);
            });
        }

        return await _client.CreateTableAsync(builder);
    }

    /// <summary>
    /// Insert rows into the provided table. The table's columns must be ints, and
    /// must have a primary key in the first column.
    /// </summary>
    /// <param name="table">The table.</param>
    /// <param name="start">The inclusive start key.</param>
    /// <param name="end">The exclusive end key.</param>
    private async Task InsertRowsAsync(KuduTable table, int start, int end)
    {
        var rows = Enumerable.Range(start, end - start).Select(i =>
        {
            var insert = table.NewInsert();

            for (int idx = 0; idx < table.Schema.Columns.Count; idx++)
                insert.SetInt32(idx, i);

            return insert;
        });

        await _client.WriteAsync(rows);
    }
}
