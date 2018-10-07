using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Kudu.Client.Builder;
using Kudu.Client.Connection;
using Kudu.Client.Exceptions;
using Kudu.Client.Internal;
using Kudu.Client.Protocol.Consensus;
using Kudu.Client.Protocol.Master;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Protocol.Tserver;
using Kudu.Client.Requests;
using Kudu.Client.Tablet;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public class KuduClient : IDisposable
    {
        private readonly KuduClientSettings _settings;
        private readonly ConnectionCache _connectionCache;
        // TODO: This needs to be thread safe. Lock or ConcurrentDictionary?
        private readonly Dictionary<string, TableLocationsCache> _tableLocations;

        private volatile ServerInfoCache _masterCache;

        public KuduClient(KuduClientSettings settings)
        {
            _settings = settings;
            _connectionCache = new ConnectionCache();
            _tableLocations = new Dictionary<string, TableLocationsCache>();
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

        public async Task<List<ListTablesResponsePB.TableInfo>> GetTablesAsync(string nameFilter = null)
        {
            var rpc = new ListTablesRequest(new ListTablesRequestPB
            {
                NameFilter = nameFilter
            });

            var result = await SendRpcToMasterAsync(rpc, ReplicaSelection.ClosestReplica);

            // TODO: Handle errors elsewhere?
            if (result.Error != null)
                throw new MasterException(result.Error);

            return result.Tables;
        }

        public async Task<List<RemoteTablet>> GetTableLocationsAsync(byte[] tableId)
        {
            var rpc = new GetTableLocationsRequest(new GetTableLocationsRequestPB
            {
                Table = new TableIdentifierPB
                {
                    TableId = tableId
                }
            });

            var result = await SendRpcToMasterAsync(rpc, ReplicaSelection.ClosestReplica);

            if (result.Error != null)
                throw new MasterException(result.Error);

            var tabletLocations = new List<RemoteTablet>(result.TabletLocations.Count);

            foreach (var tabletLocation in result.TabletLocations)
            {
                var tablet = RemoteTablet.FromTabletLocations(tableId.ToStringUtf8(), tabletLocation);
                tabletLocations.Add(tablet);
            }

            return tabletLocations;
        }

        public async Task<GetTableSchemaResponsePB> GetTableSchemaAsync(string tableName)
        {
            var rpc = new GetTableSchemaRequest(new GetTableSchemaRequestPB
            {
                Table = new TableIdentifierPB
                {
                    TableName = tableName
                }
            });

            var result = await SendRpcToMasterAsync(rpc, ReplicaSelection.ClosestReplica);

            if (result.Error != null)
                throw new MasterException(result.Error);

            return result;
        }

        public async Task<KuduTable> OpenTableAsync(string tableName)
        {
            var response = await GetTableSchemaAsync(tableName);

            return new KuduTable(response);
        }

        public async Task<WriteResponsePB> WriteRowAsync(KuduTable table, PartialRow row)
        {
            // TODO: Need to cache tablet locations.

            var rows = new byte[row.RowSize];
            var indirectData = new byte[row.IndirectDataSize];

            row.WriteTo(rows, indirectData);

            var ms = new RecyclableMemoryStream();
            KeyEncoder.EncodePartitionKey(row, table.Schema.PartitionSchema, ms);

            var rowOperations = new Protocol.RowOperationsPB
            {
                Rows = rows,
                IndirectData = indirectData
            };

            // TODO: This implementation is not complete.
            if (!_tableLocations.TryGetValue(table.Schema.TableName, out var locationCache))
            {
                // Find the tablet to send this write to.
                var locations = await GetTableLocationsAsync(table.Schema.TableId);
                locationCache = new TableLocationsCache(locations);
                _tableLocations[table.Schema.TableName] = locationCache;
            }

            var tablet = locationCache.FindTablet(ms.AsSpan());
            var serverInfo = tablet.GetServerInfo(ReplicaSelection.LeaderOnly);
            var connection = await _connectionCache.CreateConnectionAsync(serverInfo);

            var rpc = new WriteRequest(new WriteRequestPB
            {
                TabletId = tablet.TabletId.ToUtf8ByteArray(),
                Schema = table.Schema.Schema,
                RowOperations = rowOperations
            });

            var result = await SendRpcToConnectionAsync(rpc, connection);

            if (result.Error != null)
                throw new TabletServerException(result.Error);

            return result;
        }

        private async Task ConnectToClusterAsync()
        {
            var masters = new List<ServerInfo>(_settings.MasterAddresses.Count);
            int leaderIndex = -1;
            foreach (var master in _settings.MasterAddresses)
            {
                var serverInfo = CreateServerInfo("master", master);
                var connection = await _connectionCache.CreateConnectionAsync(serverInfo);
                var rpc = new ConnectToMasterRequest();
                var response = await SendRpcToConnectionAsync(rpc, connection);

                if (response.Role == RaftPeerPB.Role.Leader)
                {
                    leaderIndex = masters.Count;
                }

                masters.Add(serverInfo);
            }

            if (leaderIndex == -1)
                throw new Exception("Unable to find master leader");

            _masterCache = new ServerInfoCache(masters, leaderIndex);
        }

        private async Task<K> SendRpcToMasterAsync<T, K>(
            KuduRpc<T, K> rpc, ReplicaSelection replicaSelection)
        {
            // TODO: Don't allow this to happen in parallel.
            if (_masterCache == null)
                await ConnectToClusterAsync();

            var master = _masterCache.GetServerInfo(replicaSelection);
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

        private ServerInfo CreateServerInfo(string uuid, HostAndPort hostPort)
        {
            var ipAddresses = Dns.GetHostAddresses(hostPort.Host);
            if (ipAddresses == null || ipAddresses.Length == 0)
                throw new Exception($"Failed to resolve the IP of '{hostPort.Host}'");

            var endpoint = new IPEndPoint(ipAddresses[0], hostPort.Port);

            // TODO: Check if address is local.
            return new ServerInfo(uuid, hostPort, endpoint, false);
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
