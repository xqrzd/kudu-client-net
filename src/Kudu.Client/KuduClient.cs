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

            var result = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

            // TODO: Handle errors elsewhere?
            if (result.Error != null)
                throw new MasterException(result.Error);

            await WaitForTableDoneAsync(result.TableId).ConfigureAwait(false);

            return result.TableId;
        }

        public async Task<List<ListTablesResponsePB.TableInfo>> GetTablesAsync(string nameFilter = null)
        {
            var rpc = new ListTablesRequest(new ListTablesRequestPB
            {
                NameFilter = nameFilter
            });

            var result = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

            // TODO: Handle errors elsewhere?
            if (result.Error != null)
                throw new MasterException(result.Error);

            return result.Tables;
        }

        public async Task<List<RemoteTablet>> GetTableLocationsAsync(
            string tableId, byte[] partitionKey, uint fetchBatchSize)
        {
            // TODO: rate-limit master lookups.

            var rpc = new GetTableLocationsRequest(new GetTableLocationsRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId.ToUtf8ByteArray() },
                PartitionKeyStart = partitionKey,
                MaxReturnedLocations = fetchBatchSize
            });

            var result = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

            if (result.Error != null)
                throw new MasterException(result.Error);

            var tabletLocations = new List<RemoteTablet>(result.TabletLocations.Count);

            foreach (var tabletLocation in result.TabletLocations)
            {
                var tablet = RemoteTablet.FromTabletLocations(tableId, tabletLocation);
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

            var result = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

            if (result.Error != null)
                throw new MasterException(result.Error);

            return result;
        }

        public async Task<KuduTable> OpenTableAsync(string tableName)
        {
            var response = await GetTableSchemaAsync(tableName).ConfigureAwait(false);

            return new KuduTable(response);
        }

        public async Task<WriteResponsePB> WriteRowAsync(KuduTable table, PartialRow row)
        {
            // TODO: Need to cache tablet locations.

            var rows = new byte[row.RowSize];
            var indirectData = new byte[row.IndirectDataSize];

            row.WriteTo(rows, indirectData);

            var ms = new RecyclableMemoryStream();
            KeyEncoder.EncodePartitionKey(row, table.SchemaPb.PartitionSchema, ms);

            var rowOperations = new Protocol.RowOperationsPB
            {
                Rows = rows,
                IndirectData = indirectData
            };

            var tablet = await LookupTabletAsync(table.SchemaPb.TableId.ToStringUtf8(), ms.ToArray()).ConfigureAwait(false);
            var server = tablet.GetServerInfo(ReplicaSelection.LeaderOnly);
            var connection = await _connectionCache.CreateConnectionAsync(server).ConfigureAwait(false);

            var rpc = new WriteRequest(new WriteRequestPB
            {
                TabletId = tablet.TabletId.ToUtf8ByteArray(),
                Schema = table.SchemaPb.Schema,
                RowOperations = rowOperations
            });

            var result = await SendRpcToConnectionAsync(rpc, connection).ConfigureAwait(false);

            if (result.Error != null)
                throw new TabletServerException(result.Error);

            return result;
        }

        public ScanBuilder NewScanBuilder(KuduTable table)
        {
            return new ScanBuilder(this, table);
        }

        private async Task WaitForTableDoneAsync(byte[] tableId)
        {
            var rpc = new IsCreateTableDoneRequest(new IsCreateTableDoneRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId }
            });

            while (true)
            {
                var result = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

                if (result.Error != null)
                    throw new MasterException(result.Error);

                if (result.Done)
                    break;

                await Task.Delay(50).ConfigureAwait(false);
                // TODO: Increment rpc attempts.
            }
        }

        private async Task ConnectToClusterAsync()
        {
            var masters = new List<ServerInfo>(_settings.MasterAddresses.Count);
            int leaderIndex = -1;
            foreach (var master in _settings.MasterAddresses)
            {
                var serverInfo = CreateServerInfo("master", master);
                var connection = await _connectionCache.CreateConnectionAsync(serverInfo).ConfigureAwait(false);
                var rpc = new ConnectToMasterRequest();
                var response = await SendRpcToConnectionAsync(rpc, connection).ConfigureAwait(false);

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

        private async Task<K> SendRpcToMasterAsync<T, K>(KuduMasterRpc<T, K> rpc)
        {
            // TODO: Don't allow this to happen in parallel.
            if (_masterCache == null)
                await ConnectToClusterAsync().ConfigureAwait(false);

            var master = _masterCache.GetServerInfo(rpc.ReplicaSelection);
            var connection = await _connectionCache.CreateConnectionAsync(master).ConfigureAwait(false);

            return await SendRpcToConnectionAsync(rpc, connection).ConfigureAwait(false);
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

            CallResponse response = await connection.SendReceiveAsync(header, rpc.Request).ConfigureAwait(false);

            return rpc.ParseResponse(response);
        }

        private ValueTask<RemoteTablet> LookupTabletAsync(string tableId, byte[] partitionKey)
        {
            var tablet = FindTabletFromCache(tableId, partitionKey);
            if (tablet != null)
            {
                return new ValueTask<RemoteTablet>(tablet);
            }

            return LookupTabletSlow(tableId, partitionKey);
        }

        private async ValueTask<RemoteTablet> LookupTabletSlow(string tableId, byte[] partitionKey)
        {
            // TODO: Move to constant.
            var tablets = await GetTableLocationsAsync(tableId, partitionKey, 10).ConfigureAwait(false);

            CacheTablets(tableId, tablets, partitionKey);

            var tablet = FindTabletFromCache(tableId, partitionKey);

            if (tablet == null)
                throw new Exception("Partition range does not exist");

            return tablet;
        }

        private RemoteTablet FindTabletFromCache(string tableId, byte[] partitionKey)
        {
            RemoteTablet tablet = null;

            lock (_tableLocations)
            {
                if (_tableLocations.TryGetValue(tableId, out var cache))
                {
                    tablet = cache.FindTablet(partitionKey);
                }
            }

            return tablet;
        }

        private void CacheTablets(string tableId, List<RemoteTablet> tablets, ReadOnlySpan<byte> partitionKey)
        {
            TableLocationsCache cache;

            lock (_tableLocations)
            {
                if (!_tableLocations.TryGetValue(tableId, out cache))
                {
                    cache = new TableLocationsCache();
                    _tableLocations.Add(tableId, cache);
                }
            }

            cache.CacheTabletLocations(tablets, partitionKey);
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
            await _connectionCache.DisposeAsync().ConfigureAwait(false);
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        public static KuduClient Build(string masterAddresses)
        {
            var masters = masterAddresses.Split(',');

            var settings = new KuduClientSettings
            {
                MasterAddresses = new List<HostAndPort>(masters.Length)
            };

            foreach (var master in masters)
            {
                var hostPort = EndpointParser.TryParse(master.Trim(), 7051);
                settings.MasterAddresses.Add(hostPort);
            }

            return new KuduClient(settings);
        }
    }
}
