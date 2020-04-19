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
                await _miniCluster.DisposeAsync();
        }

        public KuduClient CreateClient() => _miniCluster.CreateClient();

        public KuduClientBuilder CreateClientBuilder() => _miniCluster.CreateClientBuilder();

        /// <summary>
        /// Helper method to easily kill the leader master.
        /// </summary>
        public async Task KillLeaderMasterServerAsync()
        {
            var hostPort = await _client.FindLeaderMasterServerAsync();
            await _miniCluster.KillMasterServerAsync(hostPort);
        }

        /// <summary>
        /// Kills and restarts the leader master.
        /// </summary>
        public async Task RestartLeaderMasterAsync()
        {
            var hostPort = await _client.FindLeaderMasterServerAsync();
            await _miniCluster.KillMasterServerAsync(hostPort);
            await _miniCluster.StartMasterServerAsync(hostPort);
        }

        /// <summary>
        /// Helper method to easily kill a tablet server that serves the given table's
        /// only tablet's leader. The currently running test case will be failed if
        /// there's more than one tablet, if the tablet has no leader after some retries,
        /// or if the tablet server was already killed.
        /// </summary>
        /// <param name="tablet">A RemoteTablet which will get its leader killed.</param>
        public async Task KillTabletLeaderAsync(RemoteTablet tablet)
        {
            var hostPort = await FindLeaderTabletServerAsync(tablet);
            await _miniCluster.KillTabletServerAsync(hostPort);
        }

        /// <summary>
        /// Kills a tablet server that serves the given tablet's leader and restarts it.
        /// </summary>
        /// <param name="tablet">A RemoteTablet which will get its leader killed and restarted.</param>
        public async Task RestartTabletServerAsync(RemoteTablet tablet)
        {
            var hostPort = await FindLeaderTabletServerAsync(tablet);
            await _miniCluster.KillTabletServerAsync(hostPort);
            await _miniCluster.StartTabletServerAsync(hostPort);
        }

        /// <summary>
        /// Starts all the master servers.
        /// Does nothing to the servers that are already running.
        /// </summary>
        public Task StartAllMasterServersAsync()
        {
            return _miniCluster.StartAllMasterServersAsync();
        }

        /// <summary>
        /// Kills all the master servers.
        /// Does nothing to the servers that are already dead.
        /// </summary>
        public Task KillAllMasterServersAsync()
        {
            return _miniCluster.KillAllMasterServersAsync();
        }

        /// <summary>
        /// Starts all the tablet servers.
        /// Does nothing to the servers that are already running.
        /// </summary>
        public Task StartAllTabletServersAsync()
        {
            return _miniCluster.StartAllTabletServersAsync();
        }

        /// <summary>
        /// Kills all the tablet servers.
        /// Does nothing to the servers that are already dead.
        /// </summary>
        public Task KillAllTabletServersAsync()
        {
            return _miniCluster.KillAllTabletServersAsync();
        }

        /// <summary>
        /// Kills a master server identified identified by an host and port.
        /// Does nothing if the master was already dead.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public Task KillMasterServerAsync(HostAndPort hostPort)
        {
            return _miniCluster.KillMasterServerAsync(hostPort);
        }

        /// <summary>
        /// Kills a tablet server identified identified by an host and port.
        /// Does nothing if the server was already dead.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public Task KillTabletServerAsync(HostAndPort hostPort)
        {
            return _miniCluster.KillTabletServerAsync(hostPort);
        }

        /// <summary>
        /// Starts a master server identified by a host and port.
        /// Does nothing if the server was already running.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public Task StartMasterServerAsync(HostAndPort hostPort)
        {
            return _miniCluster.StartMasterServerAsync(hostPort);
        }

        /// <summary>
        /// Starts a tablet server identified by a host and port.
        /// Does nothing if the server was already running.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public Task StartTabletServerAsync(HostAndPort hostPort)
        {
            return _miniCluster.StartTabletServerAsync(hostPort);
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
