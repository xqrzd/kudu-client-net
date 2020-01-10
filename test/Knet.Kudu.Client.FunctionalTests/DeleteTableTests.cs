using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class DeleteTableTests
    {
        [SkippableFact]
        public async Task CreateAndDeleteTable()
        {
            using var miniCluster = new MiniKuduClusterBuilder()
                .NumMasters(3)
                .NumTservers(3)
                .Build();

            await using var client = miniCluster.CreateClient();

            var tableName = Guid.NewGuid().ToString();
            var builder = new TableBuilder()
                .SetTableName(tableName)
                .SetNumReplicas(1)
                .AddColumn("pk", KuduType.Int32, opt => opt.Key(true));

            var table = await client.CreateTableAsync(builder);
            Assert.Equal(tableName, table.TableName);

            await client.DeleteTableAsync(tableName);

            await Assert.ThrowsAsync<NonRecoverableException>(
                () => client.DeleteTableAsync(tableName));
        }
    }
}
