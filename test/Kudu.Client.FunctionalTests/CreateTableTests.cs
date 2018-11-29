using System;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    public class CreateTableTests
    {
        [Fact]
        public async Task CreateTable()
        {
            using (var client = KuduClient.Build("127.0.0.1:7051"))
            {
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

                var tabletLocations = await client.GetTableLocationsAsync(tableId, null, 10);

                Assert.Equal(4, tabletLocations.Count);
                // TODO: Add asserts for tabletLocations contents.
            }
        }
    }
}
