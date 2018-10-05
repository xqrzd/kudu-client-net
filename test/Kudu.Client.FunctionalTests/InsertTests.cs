using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.Connection;
using Kudu.Client.Protocol;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    public class InsertTests
    {
        [Fact]
        public async Task Insert()
        {
            var settings = new KuduClientSettings
            {
                MasterAddresses = new List<HostAndPort> { new HostAndPort("127.0.0.1", 7051) }
            };

            using (var client = new KuduClient(settings))
            {
                var tableName = Guid.NewGuid().ToString();

                var table = new TableBuilder()
                    .SetTableName(tableName)
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
                    });

                var tableId = await client.CreateTableAsync(table);

                Assert.NotEmpty(tableId);

                await Task.Delay(1000);

                var openTable = await client.OpenTableAsync(tableName);
                var insert = openTable.NewInsert();
                insert.SetInt(0, 7);
                insert.SetString(1, "test value");

                var result = await client.WriteRowAsync(openTable, insert);
                Assert.Empty(result.PerRowErrors);
                Assert.NotEqual(0UL, result.Timestamp);
            }
        }
    }
}
