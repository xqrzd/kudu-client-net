using System;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ScannerTests
    {
        [SkippableFact]
        public async Task Scan()
        {
            using var miniCluster = new MiniKuduClusterBuilder()
                .NumMasters(3)
                .NumTservers(3)
                .Create();

            await using var client = miniCluster.CreateClient();

            var tableName = Guid.NewGuid().ToString();
            var builder = new TableBuilder()
                .SetTableName(tableName)
                .SetNumReplicas(1)
                .AddColumn(column =>
                {
                    column.Name = "column_x";
                    column.Type = KuduType.Int32;
                    column.IsKey = true;
                    column.IsNullable = false;
                    column.Compression = CompressionType.DefaultCompression;
                    column.Encoding = EncodingType.AutoEncoding;
                })
                .AddColumn(column =>
                {
                    column.Name = "column_y";
                    column.IsNullable = true;
                    column.Type = KuduType.String;
                    column.Encoding = EncodingType.DictEncoding;
                });

            var table = await client.CreateTableAsync(builder);
            Assert.Equal(tableName, table.TableName);
            Assert.Equal(1, table.NumReplicas);

            var insert = table.NewInsert();
            var row = insert.Row;
            row.SetInt32(0, 7);
            row.SetString(1, "test value");

            var results = await client.WriteRowAsync(new[] { insert });
            Assert.Collection(results, r =>
            {
                Assert.Empty(r.PerRowErrors);
                Assert.NotEqual(0UL, r.Timestamp);
            });

            var scanner = client.NewScanBuilder(table)
                .SetProjectedColumns("column_x", "column_y")
                .Build();

            await foreach (var resultSet in scanner)
            {
                CheckResults(resultSet);
            }

            static void CheckResults(ResultSet rows)
            {
                Assert.Equal(1, rows.Count);

                foreach (var row in rows)
                {
                    Assert.Equal(7, row.GetInt32(0));
                    Assert.Equal("test value", row.GetString(1));
                }
            }
        }
    }
}
