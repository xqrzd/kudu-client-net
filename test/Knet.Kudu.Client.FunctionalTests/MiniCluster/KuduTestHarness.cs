using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Tablet;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    public class KuduTestHarness : IAsyncDisposable
    {
        private readonly MiniKuduCluster _miniCluster;
        private readonly bool _disposeMiniCluster;
        private readonly KuduClient _client;

        public KuduTestHarness(
            MiniKuduCluster miniCluster,
            bool disposeMiniCluster = false)
        {
            _miniCluster = miniCluster;
            _disposeMiniCluster = disposeMiniCluster;
            _client = miniCluster.CreateClient();
        }

        public async ValueTask DisposeAsync()
        {
            await _client.DisposeAsync();

            if (_disposeMiniCluster)
                _miniCluster.Dispose();
        }

        public KuduClient CreateClient() => _miniCluster.CreateClient();

        /// <summary>
        /// Helper method to easily kill the leader master.
        /// </summary>
        public async ValueTask KillLeaderMasterServerAsync()
        {
            var hostPort = await _client.FindLeaderMasterServerAsync();
            _miniCluster.KillMasterServer(hostPort);
        }

        /// <summary>
        /// Kills and restarts the leader master.
        /// </summary>
        public async ValueTask RestartLeaderMasterAsync()
        {
            var hostPort = await _client.FindLeaderMasterServerAsync();
            _miniCluster.KillMasterServer(hostPort);
            _miniCluster.StartMasterServer(hostPort);
        }

        /// <summary>
        /// Helper method to easily kill a tablet server that serves the given table's
        /// only tablet's leader. The currently running test case will be failed if
        /// there's more than one tablet, if the tablet has no leader after some retries,
        /// or if the tablet server was already killed.
        /// </summary>
        /// <param name="tablet">A RemoteTablet which will get its leader killed.</param>
        public async ValueTask KillTabletLeaderAsync(RemoteTablet tablet)
        {
            var hostPort = await FindLeaderTabletServerAsync(tablet);
            _miniCluster.KillTabletServer(hostPort);
        }

        /// <summary>
        /// Kills a tablet server that serves the given tablet's leader and restarts it.
        /// </summary>
        /// <param name="tablet">A RemoteTablet which will get its leader killed and restarted.</param>
        public async ValueTask RestartTabletServerAsync(RemoteTablet tablet)
        {
            var hostPort = await FindLeaderTabletServerAsync(tablet);
            _miniCluster.KillTabletServer(hostPort);
            _miniCluster.StartTabletServer(hostPort);
        }

        /// <summary>
        /// Starts all the tablet servers.
        /// Does nothing to the servers that are already running.
        /// </summary>
        public void StartAllTabletServers()
        {
            _miniCluster.StartAllTabletServers();
        }

        /// <summary>
        /// Finds the RPC port of the given tablet's leader tserver.
        /// </summary>
        /// <param name="tablet">The remote tablet.</param>
        public async ValueTask<HostAndPort> FindLeaderTabletServerAsync(RemoteTablet tablet)
        {
            var leader = tablet.GetLeaderServerInfo();

            while (leader == null)
            {
                await Task.Delay(100);

                var tablets = await _client.GetTableLocationsAsync(
                    tablet.TableId, tablet.Partition.PartitionKeyStart, 1);

                Assert.Single(tablets);

                leader = tablets[0].GetLeaderServerInfo();
            }

            return leader.HostPort;
        }
    }
}
