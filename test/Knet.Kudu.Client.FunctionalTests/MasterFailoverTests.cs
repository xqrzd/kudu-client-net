using System.Threading.Tasks;
using Knet.Kudu.Client.Builder;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class MasterFailoverTests
    {
        [SkippableTheory]
        [InlineData(KillBefore.CreateTable, true)]
        [InlineData(KillBefore.CreateTable, false)]
        [InlineData(KillBefore.OpenTable, true)]
        [InlineData(KillBefore.OpenTable, false)]
        [InlineData(KillBefore.ScanTable, true)]
        [InlineData(KillBefore.ScanTable, false)]
        public async Task TestMasterFailover(KillBefore killBefore, bool restart)
        {
            using var miniCluster = new MiniKuduClusterBuilder()
                .NumMasters(3)
                .NumTservers(3)
                .Build();

            await using var client = miniCluster.CreateClient();

            // Hack-fix to discover which master is the leader.
            await client.GetTablesAsync();

            if (killBefore == KillBefore.CreateTable)
                DoAction();

            var tableName = $"TestMasterFailover-killBefore={killBefore}";
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
                })
                .AddHashPartitions(buckets: 4, seed: 777, "column_x");

            var table = await client.CreateTableAsync(builder);
            Assert.Equal(tableName, table.TableName);

            if (killBefore == KillBefore.OpenTable)
                DoAction();

            var table2 = await client.OpenTableAsync(tableName);

            if (killBefore == KillBefore.ScanTable)
                DoAction();

            var scanner = client.NewScanBuilder(table2)
                .Build();

            await foreach (var resultSet in scanner)
            {
                Assert.Equal(0, resultSet.Count);
            }

            void DoAction()
            {
                if (restart)
                    RestartLeaderMaster(miniCluster, client);
                else
                    KillLeaderMasterServer(miniCluster, client);
            }
        }

        // TODO: Move this to a shared class.
        private void KillLeaderMasterServer(
            MiniKuduCluster miniCluster, KuduClient client)
        {
            var leader = client.GetMasterServerInfo(ReplicaSelection.LeaderOnly);
            miniCluster.KillMasterServer(leader.HostPort);
        }

        private void RestartLeaderMaster(
            MiniKuduCluster miniCluster, KuduClient client)
        {
            var leader = client.GetMasterServerInfo(ReplicaSelection.LeaderOnly);
            miniCluster.KillMasterServer(leader.HostPort);
            miniCluster.StartMasterServer(leader.HostPort);
        }

        public enum KillBefore
        {
            CreateClient,
            CreateTable,
            OpenTable,
            ScanTable
        }
    }
}
