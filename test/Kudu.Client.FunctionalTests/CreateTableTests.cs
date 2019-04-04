using System;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.FunctionalTests.MiniCluster;
using Kudu.Client.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    public class CreateTableTests : MiniKuduClusterTestBase
    {
        [SkippableFact]
        public async Task CreateTable()
        {
            var client = GetKuduClient();

            var table = new TableBuilder()
                .SetTableName(Guid.NewGuid().ToString())
                .SetNumReplicas(1)
                .AddColumn(column =>
                {
                    column.Name = "column_x";
                    column.Type = DataType.Int32;
                    column.IsKey = true;
                    column.IsNullable = false;
                    column.Compression = CompressionType.DefaultCompression;
                    column.Encoding = EncodingType.AutoEncoding;
                })
                .AddColumn(column =>
                {
                    column.Name = "column_y";
                    column.IsNullable = true;
                    column.Type = DataType.String;
                    column.Encoding = EncodingType.DictEncoding;
                })
                .AddHashPartitions(buckets: 4, "column_x");

            var tableId = await client.CreateTableAsync(table);

            Assert.NotEmpty(tableId);

            var tables = await client.GetTablesAsync();

            Assert.Contains(tables,
                t => t.Id.AsSpan().SequenceEqual(tableId));

            var tabletLocations = await client.GetTableLocationsAsync(tableId.ToStringUtf8(), null, 10);

            Assert.Equal(4, tabletLocations.Count);
            // TODO: Add asserts for tabletLocations contents.
        }

        [SkippableFact]
        public async Task CreateTableWithRangePartitions()
        {
            var client = GetKuduClient();

            var tableName = Guid.NewGuid().ToString();
            var builder = new TableBuilder()
                .SetTableName(tableName)
                .SetNumReplicas(1)
                .AddColumn(column =>
                {
                    column.Name = "column_x";
                    column.Type = DataType.Int32;
                    column.IsKey = true;
                })
                .AddColumn(column =>
                {
                    column.Name = "column_y";
                    column.Type = DataType.String;
                    column.IsKey = true;
                })
                .SetRangePartitionColumns("column_x")
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt(0, 20);
                    upper.SetInt(0, 21);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt(0, 30);
                    upper.SetInt(0, 50);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt(0, 67);
                    upper.SetInt(0, 99);
                });

            var tableId = await client.CreateTableAsync(builder);
            Assert.NotEmpty(tableId);

            var tables = await client.GetTablesAsync(tableName);
            Assert.Contains(tables,
                t => t.Id.AsSpan().SequenceEqual(tableId));

            var table = await client.OpenTableAsync(tableName);
            var partition = table.PartitionSchema;
            Assert.Empty(partition.HashBucketSchemas);
            Assert.Equal(new int[] { 0 }, partition.RangeSchema.ColumnIds);

            var locations = await client.GetTableLocationsAsync(tableId.ToStringUtf8(), null, 10);
            Assert.Collection(locations, t =>
            {
                var start = new byte[] { 0x80, 0x0, 0x0, 0x14 };
                var end = new byte[] { 0x80, 0x0, 0x0, 0x15 };

                Assert.Equal(start, t.Partition.PartitionKeyStart);
                Assert.Equal(start, t.Partition.RangeKeyStart);
                Assert.Equal(end, t.Partition.PartitionKeyEnd);
                Assert.Equal(end, t.Partition.RangeKeyEnd);
            }, t =>
            {
                var start = new byte[] { 0x80, 0x0, 0x0, 0x1E };
                var end = new byte[] { 0x80, 0x0, 0x0, 0x32 };

                Assert.Equal(start, t.Partition.PartitionKeyStart);
                Assert.Equal(start, t.Partition.RangeKeyStart);
                Assert.Equal(end, t.Partition.PartitionKeyEnd);
                Assert.Equal(end, t.Partition.RangeKeyEnd);
            }, t =>
            {
                var start = new byte[] { 0x80, 0x0, 0x0, 0x43 };
                var end = new byte[] { 0x80, 0x0, 0x0, 0x63 };

                Assert.Equal(start, t.Partition.PartitionKeyStart);
                Assert.Equal(start, t.Partition.RangeKeyStart);
                Assert.Equal(end, t.Partition.PartitionKeyEnd);
                Assert.Equal(end, t.Partition.RangeKeyEnd);
            });
        }
    }
}
