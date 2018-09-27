using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.Connection;
using Kudu.Client.Protocol;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    public class CreateTableTests
    {
        [Fact]
        public async Task CreateTable()
        {
            var settings = new KuduClientSettings
            {
                MasterAddresses = new List<HostAndPort> { new HostAndPort("localhost", 7051) }
            };

            using (var client = new KuduClient(settings))
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
                    .AddHashPartition(buckets: 4, "column_x");

                var tableId = await client.CreateTableAsync(table);

                Assert.NotEmpty(tableId);
            }
        }
    }
}
