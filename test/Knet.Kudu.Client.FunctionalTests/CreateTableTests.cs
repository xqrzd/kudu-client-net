using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class CreateTableTests
{
    [SkippableFact]
    public async Task CreateTableWithHashPartitions()
    {
        await using var miniCluster = await new MiniKuduClusterBuilder()
            .NumMasters(3)
            .NumTservers(3)
            .BuildAsync();

        await using var client = miniCluster.CreateClient();

        var tableName = Guid.NewGuid().ToString();
        var builder = new TableBuilder()
            .SetTableName(tableName)
            .SetNumReplicas(1)
            .AddColumn("column_x", KuduType.Int32, opt => opt
                .Key(true)
                .Nullable(false))
            .AddColumn("column_y", KuduType.String, opt => opt
                .Encoding(EncodingType.DictEncoding))
            .AddHashPartitions(buckets: 4, seed: 777, "column_x");

        var table = await client.CreateTableAsync(builder);
        Assert.Equal(tableName, table.TableName);

        var partitionSchema = table.PartitionSchema;
        Assert.Collection(partitionSchema.HashBucketSchemas, h =>
        {
            Assert.Equal(4, h.NumBuckets);
            Assert.Equal(777ul, h.Seed);
            Assert.Equal(new int[] { 0 }, h.ColumnIds);
        });
        Assert.Empty(partitionSchema.RangeSchema.ColumnIds);

        var locations = await client.GetTableLocationsAsync(table.TableId, null, 10);
        Assert.Collection(locations, t =>
        {
            var start = new byte[0];
            var end = new byte[] { 0x00, 0x00, 0x00, 0x01 };

            Assert.Equal(new int[] { 0 }, t.Partition.HashBuckets);

            Assert.True(t.Partition.IsStartPartition);
            Assert.False(t.Partition.IsEndPartition);

            Assert.Equal(start, t.Partition.PartitionKeyStart);
            Assert.Equal(end, t.Partition.PartitionKeyEnd);

            Assert.Empty(t.Partition.RangeKeyStart);
            Assert.Empty(t.Partition.RangeKeyEnd);
        }, t =>
        {
            var start = new byte[] { 0x00, 0x00, 0x00, 0x01 };
            var end = new byte[] { 0x00, 0x00, 0x00, 0x02 };

            Assert.Equal(new int[] { 1 }, t.Partition.HashBuckets);

            Assert.False(t.Partition.IsStartPartition);
            Assert.False(t.Partition.IsEndPartition);

            Assert.Equal(start, t.Partition.PartitionKeyStart);
            Assert.Equal(end, t.Partition.PartitionKeyEnd);

            Assert.Empty(t.Partition.RangeKeyStart);
            Assert.Empty(t.Partition.RangeKeyEnd);
        }, t =>
        {
            var start = new byte[] { 0x00, 0x00, 0x00, 0x02 };
            var end = new byte[] { 0x00, 0x00, 0x00, 0x03 };

            Assert.Equal(new int[] { 2 }, t.Partition.HashBuckets);

            Assert.False(t.Partition.IsStartPartition);
            Assert.False(t.Partition.IsEndPartition);

            Assert.Equal(start, t.Partition.PartitionKeyStart);
            Assert.Equal(end, t.Partition.PartitionKeyEnd);

            Assert.Empty(t.Partition.RangeKeyStart);
            Assert.Empty(t.Partition.RangeKeyEnd);
        }, t =>
        {
            var start = new byte[] { 0x00, 0x00, 0x00, 0x03 };
            var end = new byte[0];

            Assert.Equal(new int[] { 3 }, t.Partition.HashBuckets);

            Assert.False(t.Partition.IsStartPartition);
            Assert.True(t.Partition.IsEndPartition);

            Assert.Equal(start, t.Partition.PartitionKeyStart);
            Assert.Equal(end, t.Partition.PartitionKeyEnd);

            Assert.Empty(t.Partition.RangeKeyStart);
            Assert.Empty(t.Partition.RangeKeyEnd);
        });
    }

    [SkippableFact]
    public async Task CreateTableWithRangePartitions()
    {
        await using var miniCluster = await new MiniKuduClusterBuilder()
            .NumMasters(3)
            .NumTservers(3)
            .BuildAsync();

        await using var client = miniCluster.CreateClient();

        var tableName = Guid.NewGuid().ToString();
        var builder = new TableBuilder()
            .SetTableName(tableName)
            .SetNumReplicas(1)
            .AddColumn("column_x", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("column_y", KuduType.String, opt => opt.Key(true))
            .SetRangePartitionColumns("column_x")
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32(0, 20);
                upper.SetInt32(0, 21);
            })
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32(0, 30);
                upper.SetInt32(0, 50);
            })
            .AddRangePartition((lower, upper) =>
            {
                lower.SetInt32(0, 67);
                upper.SetInt32(0, 99);
            });

        var table = await client.CreateTableAsync(builder);
        Assert.Equal(tableName, table.TableName);

        var partitionSchema = table.PartitionSchema;
        Assert.Equal(new int[] { 0 }, partitionSchema.RangeSchema.ColumnIds);
        Assert.Empty(partitionSchema.HashBucketSchemas);

        var locations = await client.GetTableLocationsAsync(table.TableId, null, 10);
        Assert.Collection(locations, t =>
        {
            var start = new byte[] { 0x80, 0x0, 0x0, 0x14 };
            var end = new byte[] { 0x80, 0x0, 0x0, 0x15 };

            Assert.Equal(start, t.Partition.PartitionKeyStart);
            Assert.Equal(start, t.Partition.RangeKeyStart);
            Assert.Equal(end, t.Partition.PartitionKeyEnd);
            Assert.Equal(end, t.Partition.RangeKeyEnd);

            Assert.False(t.Partition.IsStartPartition);
            Assert.False(t.Partition.IsEndPartition);
            Assert.Empty(t.Partition.HashBuckets);
        }, t =>
        {
            var start = new byte[] { 0x80, 0x0, 0x0, 0x1E };
            var end = new byte[] { 0x80, 0x0, 0x0, 0x32 };

            Assert.Equal(start, t.Partition.PartitionKeyStart);
            Assert.Equal(start, t.Partition.RangeKeyStart);
            Assert.Equal(end, t.Partition.PartitionKeyEnd);
            Assert.Equal(end, t.Partition.RangeKeyEnd);

            Assert.False(t.Partition.IsStartPartition);
            Assert.False(t.Partition.IsEndPartition);
            Assert.Empty(t.Partition.HashBuckets);
        }, t =>
        {
            var start = new byte[] { 0x80, 0x0, 0x0, 0x43 };
            var end = new byte[] { 0x80, 0x0, 0x0, 0x63 };

            Assert.Equal(start, t.Partition.PartitionKeyStart);
            Assert.Equal(start, t.Partition.RangeKeyStart);
            Assert.Equal(end, t.Partition.PartitionKeyEnd);
            Assert.Equal(end, t.Partition.RangeKeyEnd);

            Assert.False(t.Partition.IsStartPartition);
            Assert.False(t.Partition.IsEndPartition);
            Assert.Empty(t.Partition.HashBuckets);
        });
    }
}
