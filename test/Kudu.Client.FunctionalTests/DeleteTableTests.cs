using System;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.Exceptions;
using Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Kudu.Client.FunctionalTests
{
    public class DeleteTableTests : MiniKuduClusterTestBase
    {
        [SkippableFact]
        public async Task CreateAndDeleteTable()
        {
            var client = GetKuduClient();

            var tableName = Guid.NewGuid().ToString();
            var builder = new TableBuilder()
                .SetTableName(tableName)
                .SetNumReplicas(1)
                .AddColumn(column =>
                {
                    column.Name = "pk";
                    column.Type = DataType.Int32;
                    column.IsKey = true;
                    column.IsNullable = false;
                });

            var table = await client.CreateTableAsync(builder);
            Assert.Equal(tableName, table.TableName);

            await client.DeleteTableAsync(tableName);

            await Assert.ThrowsAsync<MasterException>(() => client.DeleteTableAsync(tableName));
        }
    }
}
