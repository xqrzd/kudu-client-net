using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.Connection;
using Kudu.Client.Exceptions;
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

        public async Task<byte[]> CreateTableAsync(TableBuilder table)
        {
            var rpc = new CreateTableRequest(table);

            var result = await SendRpcToMasterAsync(rpc, ReplicaSelection.LeaderOnly);

            // TODO: Handle errors elsewhere?
            if (result.Error != null)
                throw new MasterException(result.Error);

            return result.TableId;
        }

        private async Task ConnectToClusterAsync()
        {
            var masters = new List<HostAndPort>(_settings.MasterAddresses.Count);
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

                masters.Add(master);
            }

            if (leaderIndex == -1)
                throw new Exception("Unable to find master leader");

            _masterCache = new MasterCache(masters, leaderIndex);
        }

        private async Task<K> SendRpcToMasterAsync<T, K>(
            KuduRpc<T, K> rpc, ReplicaSelection replicaSelection)
        {
            // TODO: Don't allow this to happen in parallel.
            if (_masterCache == null)
                await ConnectToClusterAsync();

            var master = _masterCache.GetMasterInfo(replicaSelection);
            var connection = await _connectionCache.CreateConnectionAsync(master);

            return await SendRpcToConnectionAsync(rpc, connection);
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

        public async Task DisposeAsync()
        {
            await _connectionCache.DisposeAsync();
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }
    }
}
