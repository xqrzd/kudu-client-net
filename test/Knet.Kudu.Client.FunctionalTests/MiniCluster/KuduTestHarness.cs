using System;
using System.Threading.Tasks;

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
    }
}
