using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kudu.Client.Connection;
using Kudu.Client.Protocol.Consensus;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Requests;

namespace Kudu.Client
{
    public class KuduClient : IDisposable
    {
        private readonly ConnectionCache _connectionCache;
        private readonly KuduClientSettings _settings;

        private volatile MasterCache _masterCache;

        public KuduClient(KuduClientSettings settings)
        {
            _connectionCache = new ConnectionCache();
            _settings = settings;
        }

        private async Task ConnectToClusterAsync()
        {
            var masters = new List<ServerInfo>(_settings.MasterAddresses.Count);
            int leaderIndex = -1;
            foreach (var master in _settings.MasterAddresses)
            {
                var connection = await _connectionCache.CreateConnectionAsync(master);
                var rpc = new ConnectToMasterRequest();
                var response = await SendRpcToConnectionAsync(rpc, connection);

                if (response.Role == RaftPeerPB.Role.Leader)
                {
                    leaderIndex = masters.Count;
                }

                var serverInfo = new ServerInfo($"master-{master}", master);
                masters.Add(serverInfo);
            }

            if (leaderIndex == -1)
                throw new Exception("Unable to find master leader");

            _masterCache = new MasterCache(masters, leaderIndex);
        }

        private async Task<K> SendRpcToConnectionAsync<T, K>(
            KuduRpc<T, K> rpc, KuduConnection connection)
        {
            var header = new RequestHeader
            {
                // CallId is set by KuduConnection.
                RemoteMethod = new RemoteMethodPB
                {
                    ServiceName = rpc.ServiceName,
                    MethodName = rpc.MethodName
                }
                // TODO: Set RequiredFeatureFlags
            };

            CallResponse response = await connection.SendReceiveAsync(header, rpc.Request);

            return rpc.ParseResponse(response);
        }

        public void Dispose()
        {
            _connectionCache.Dispose();
        }
    }
}
