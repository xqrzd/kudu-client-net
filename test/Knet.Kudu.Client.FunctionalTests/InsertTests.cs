using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class InsertTests
    {
        [SkippableFact]
        public async Task Insert()
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
                .AddColumn("column_y", KuduType.String);

            var table = await client.CreateTableAsync(builder);
            Assert.Equal(tableName, table.TableName);
            Assert.Equal(1, table.NumReplicas);

            var row = table.NewInsert();
            row.SetInt32(0, 7);
            row.SetString(1, "test value");

            var results = await client.WriteAsync(new[] { row });
            Assert.Collection(results, r =>
            {
                Assert.Empty(r.PerRowErrors);
                Assert.NotEqual(0UL, r.Timestamp);
            });
        }
    }
}
