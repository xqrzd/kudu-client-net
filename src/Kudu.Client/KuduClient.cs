using System;
using System.Collections.Generic;
using System.Threading;
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
    public class KuduClient : IAsyncDisposable
    {
        /// <summary>
        /// The number of tablets to fetch from the master in a round trip when
        /// performing a lookup of a single partition (e.g. for a write), or
        /// re-looking-up a tablet with stale information.
        /// </summary>
        private const int FetchTabletsPerPointLookup = 10;
        private const int MaxRpcAttempts = 100;

        public const long NoTimestamp = -1;
        public const long DefaultOperationTimeoutMs = 30000;
        public const long DefaultKeepAlivePeriodMs = 15000; // 25% of the default scanner ttl.

        private readonly KuduClientOptions _options;
        private readonly IKuduConnectionFactory _connectionFactory;
        private readonly ConnectionCache _connectionCache;
        private readonly Dictionary<string, TableLocationsCache> _tableLocations;
        private readonly RequestTracker _requestTracker;

        private volatile bool _hasConnectedToMaster;
        private volatile string _location;
        private volatile ServerInfoCache _masterCache;
        private volatile HiveMetastoreConfig _hiveMetastoreConfig;

        /// <summary>
        /// Timestamp required for HybridTime external consistency through timestamp propagation.
        /// </summary>
        private long _lastPropagatedTimestamp = NoTimestamp;
        private readonly object _lastPropagatedTimestampLock = new object();

        public KuduClient(KuduClientOptions options)
        {
            _options = options;
            _connectionFactory = new KuduConnectionFactory();
            _connectionCache = new ConnectionCache(_connectionFactory);
            _tableLocations = new Dictionary<string, TableLocationsCache>();
            _requestTracker = new RequestTracker(Guid.NewGuid().ToString("N"));
        }

        /// <summary>
        /// The last timestamp received from a server. Used for CLIENT_PROPAGATED
        /// external consistency. Note that the returned timestamp is encoded and
        /// cannot be interpreted as a raw timestamp.
        /// </summary>
        public long LastPropagatedTimestamp
        {
            get
            {
                lock (_lastPropagatedTimestampLock)
                {
                    return _lastPropagatedTimestamp;
                }
            }
            set
            {
                lock (_lastPropagatedTimestampLock)
                {
                    if (_lastPropagatedTimestamp == NoTimestamp ||
                        _lastPropagatedTimestamp < value)
                    {
                        _lastPropagatedTimestamp = value;
                    }
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            return _connectionCache.DisposeAsync();
        }

        /// <summary>
        /// Get the Hive Metastore configuration of the most recently connected-to leader master,
        /// or null if the Hive Metastore integration is not enabled.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        public ValueTask<HiveMetastoreConfig> GetHiveMetastoreConfigAsync(
            CancellationToken cancellationToken = default)
        {
            if (_hasConnectedToMaster)
            {
                return new ValueTask<HiveMetastoreConfig>(_hiveMetastoreConfig);
            }

            return ConnectAsync(cancellationToken);

            async ValueTask<HiveMetastoreConfig> ConnectAsync(CancellationToken cancellationToken)
            {
                await ConnectToClusterAsync(cancellationToken).ConfigureAwait(false);
                return _hiveMetastoreConfig;
            }
        }

        public async Task<KuduTable> CreateTableAsync(TableBuilder table)
        {
            var rpc = new CreateTableRequest(table.Build());

            await SendRpcToMasterAsync(rpc).ConfigureAwait(false);
            var result = rpc.Response;

            // TODO: Handle errors elsewhere?
            if (result.Error != null)
                throw new MasterException(result.Error);

            await WaitForTableDoneAsync(result.TableId).ConfigureAwait(false);

            var tableIdentifier = new TableIdentifierPB { TableId = result.TableId };
            return await OpenTableAsync(tableIdentifier).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete a table on the cluster with the specified name.
        /// </summary>
        /// <param name="tableName">The table's name.</param>
        /// <param name="modifyExternalCatalogs">
        /// Whether to apply the deletion to external catalogs, such as the Hive Metastore.
        /// </param>
        public async Task DeleteTableAsync(string tableName, bool modifyExternalCatalogs = true)
        {
            var rpc = new DeleteTableRequest(new DeleteTableRequestPB
            {
                Table = new TableIdentifierPB { TableName = tableName },
                ModifyExternalCatalogs = modifyExternalCatalogs
            });

            await SendRpcToMasterAsync(rpc).ConfigureAwait(false);
            var result = rpc.Response;

            // TODO: Handle errors elsewhere?
            if (result.Error != null)
                throw new MasterException(result.Error);
        }

        public async Task<List<ListTablesResponsePB.TableInfo>> GetTablesAsync(string nameFilter = null)
        {
            var rpc = new ListTablesRequest(new ListTablesRequestPB
            {
                NameFilter = nameFilter
            });

            await SendRpcToMasterAsync(rpc).ConfigureAwait(false);
            var result = rpc.Response;

            // TODO: Handle errors elsewhere?
            if (result.Error != null)
                throw new MasterException(result.Error);

            return result.Tables;
        }

        public async Task<List<RemoteTablet>> GetTableLocationsAsync(
            string tableId, byte[] partitionKey, uint fetchBatchSize,
            CancellationToken cancellationToken = default)
        {
            // TODO: rate-limit master lookups.

            var rpc = new GetTableLocationsRequest(new GetTableLocationsRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId.ToUtf8ByteArray() },
                PartitionKeyStart = partitionKey,
                MaxReturnedLocations = fetchBatchSize
            });

            await SendRpcToMasterAsync(rpc, cancellationToken).ConfigureAwait(false);
            var result = rpc.Response;

            if (result.Error != null)
                throw new MasterException(result.Error);

            var tabletLocations = new List<RemoteTablet>(result.TabletLocations.Count);

            foreach (var tabletLocation in result.TabletLocations)
            {
                var tablet = await _connectionFactory.GetTabletAsync(tableId, tabletLocation)
                    .ConfigureAwait(false);
                tabletLocations.Add(tablet);
            }

            return tabletLocations;
        }

        public Task<GetTableSchemaResponsePB> GetTableSchemaAsync(string tableName)
        {
            var tableIdentifier = new TableIdentifierPB { TableName = tableName };
            return GetTableSchemaAsync(tableIdentifier);
        }

        public async Task<KuduTable> OpenTableAsync(string tableName)
        {
            var tableIdentifier = new TableIdentifierPB { TableName = tableName };
            var response = await GetTableSchemaAsync(tableIdentifier).ConfigureAwait(false);

            return new KuduTable(response);
        }

        public async Task<WriteResponsePB> WriteRowAsync(Operation operation)
        {
            var row = operation.Row;
            var table = operation.Table;
            var rows = new byte[row.RowSize];
            var indirectData = new byte[row.IndirectDataSize];

            row.WriteTo(rows, indirectData);

            var rowOperations = new Protocol.RowOperationsPB
            {
                Rows = rows,
                IndirectData = indirectData
            };

            var tablet = await GetRowTabletAsync(table, row).ConfigureAwait(false);
            if (tablet == null)
                throw new Exception("The requested tablet does not exist");

            var rpc = new WriteRequest(new WriteRequestPB
            {
                TabletId = tablet.TabletId.ToUtf8ByteArray(),
                Schema = table.SchemaPb.Schema,
                RowOperations = rowOperations
            }, table.TableId);

            // TODO: Avoid double tablet lookup.
            await SendRpcToTabletAsync(rpc).ConfigureAwait(false);
            var result = rpc.Response;

            if (result.Error != null)
                throw new TabletServerException(result.Error);

            return result;
        }

        public async Task<WriteResponsePB[]> WriteRowAsync(IEnumerable<Operation> operations)
        {
            var operationsByTablet = new Dictionary<RemoteTablet, List<Operation>>();

            foreach (var operation in operations)
            {
                var tablet = await GetRowTabletAsync(operation.Table, operation.Row)
                    .ConfigureAwait(false);

                if (tablet != null)
                {
                    if (!operationsByTablet.TryGetValue(tablet, out var tabletOperations))
                    {
                        tabletOperations = new List<Operation>();
                        operationsByTablet.Add(tablet, tabletOperations);
                    }

                    tabletOperations.Add(operation);
                }
                else
                {
                    // TODO: Handle failure
                    Console.WriteLine("Unable to find tablet");
                }
            }

            var tasks = new Task<WriteResponsePB>[operationsByTablet.Count];
            var i = 0;

            foreach (var tabletOperations in operationsByTablet)
            {
                var task = WriteRowAsync(tabletOperations.Value, tabletOperations.Key);
                tasks[i++] = task;
            }

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            return results;
        }

        private async Task<WriteResponsePB> WriteRowAsync(List<Operation> operations, RemoteTablet tablet)
        {
            var table = operations[0].Table;

            byte[] rowData;
            byte[] indirectData;

            // TODO: Estimate better sizes for these.
            using (var rowBuffer = new BufferWriter(1024))
            using (var indirectBuffer = new BufferWriter(1024))
            {
                OperationsEncoder.Encode(operations, rowBuffer, indirectBuffer);

                // protobuf-net doesn't support serializing Memory<byte>,
                // so we need to copy these into an array.
                rowData = rowBuffer.Memory.ToArray();
                indirectData = indirectBuffer.Memory.ToArray();
            }

            var rowOperations = new Protocol.RowOperationsPB
            {
                Rows = rowData,
                IndirectData = indirectData
            };

            var rpc = new WriteRequest(new WriteRequestPB
            {
                TabletId = tablet.TabletId.ToUtf8ByteArray(),
                Schema = table.SchemaPb.Schema,
                RowOperations = rowOperations
            }, table.TableId);

            await SendRpcToTabletAsync(rpc).ConfigureAwait(false);
            var result = rpc.Response;

            if (result.Error != null)
                throw new TabletServerException(result.Error);

            return result;
        }

        public ScanBuilder NewScanBuilder(KuduTable table)
        {
            return new ScanBuilder(this, table);
        }

        private async Task<KuduTable> OpenTableAsync(TableIdentifierPB tableIdentifier)
        {
            var response = await GetTableSchemaAsync(tableIdentifier).ConfigureAwait(false);

            return new KuduTable(response);
        }

        private async Task<GetTableSchemaResponsePB> GetTableSchemaAsync(TableIdentifierPB tableIdentifier)
        {
            var rpc = new GetTableSchemaRequest(new GetTableSchemaRequestPB
            {
                Table = tableIdentifier
            });

            await SendRpcToMasterAsync(rpc).ConfigureAwait(false);
            var result = rpc.Response;

            if (result.Error != null)
                throw new MasterException(result.Error);

            return result;
        }

        private async Task WaitForTableDoneAsync(byte[] tableId)
        {
            var rpc = new IsCreateTableDoneRequest(new IsCreateTableDoneRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId }
            });

            while (true)
            {
                await SendRpcToMasterAsync(rpc).ConfigureAwait(false);
                var result = rpc.Response;

                if (result.Error != null)
                    throw new MasterException(result.Error);

                if (result.Done)
                    break;

                await Task.Delay(50).ConfigureAwait(false);
                // TODO: Increment rpc attempts.
            }
        }

        private ValueTask<RemoteTablet> GetRowTabletAsync(KuduTable table, PartialRow row)
        {
            using var writer = new BufferWriter(256);
            KeyEncoder.EncodePartitionKey(row, table.PartitionSchema, writer);
            var partitionKey = writer.Memory.Span;

            // Note that we don't have to await this method before disposing the writer, as a
            // copy of partitionKey will be made if the method cannot complete synchronously.
            return GetTabletAsync(table.TableId, partitionKey);
        }

        /// <summary>
        /// Locates a tablet by consulting the table location cache, then by contacting
        /// a master if we haven't seen the tablet before. The results are cached.
        /// </summary>
        /// <param name="tableId">The table identifier.</param>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The requested tablet, or null if the tablet doesn't exist.</returns>
        private ValueTask<RemoteTablet> GetTabletAsync(
            string tableId, ReadOnlySpan<byte> partitionKey,
            CancellationToken cancellationToken = default)
        {
            var tablet = GetTabletFromCache(tableId, partitionKey);

            if (tablet != null)
                return new ValueTask<RemoteTablet>(tablet);

            var task = LookupAndCacheTabletAsync(tableId, partitionKey.ToArray(), cancellationToken);
            return new ValueTask<RemoteTablet>(task);
        }

        /// <summary>
        /// Locates a tablet by consulting the table location cache.
        /// </summary>
        /// <param name="tableId">The table identifier.</param>
        /// <param name="partitionKey">The partition key.</param>
        /// <returns>The requested tablet, or null if the tablet doesn't exist.</returns>
        private RemoteTablet GetTabletFromCache(string tableId, ReadOnlySpan<byte> partitionKey)
        {
            TableLocationsCache tableCache;

            lock (_tableLocations)
            {
                if (!_tableLocations.TryGetValue(tableId, out tableCache))
                {
                    // We don't have any tablets cached for this table.
                    return null;
                }
            }

            return tableCache.FindTablet(partitionKey);
        }

        /// <summary>
        /// Locates a tablet by consulting a master and caches the results.
        /// </summary>
        /// <param name="tableId">The table identifier.</param>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The requested tablet, or null if the tablet doesn't exist.</returns>
        private async Task<RemoteTablet> LookupAndCacheTabletAsync(
            string tableId, byte[] partitionKey, CancellationToken cancellationToken = default)
        {
            var tablets = await GetTableLocationsAsync(
                tableId,
                partitionKey,
                FetchTabletsPerPointLookup,
                cancellationToken).ConfigureAwait(false);

            CacheTablets(tableId, tablets, partitionKey);

            var tablet = GetTabletFromCache(tableId, partitionKey);

            return tablet;
        }

        /// <summary>
        /// Adds the given tablets to the table location cache.
        /// </summary>
        /// <param name="tableId">The table identifier.</param>
        /// <param name="tablets">The tablets to cache.</param>
        /// <param name="partitionKey">The partition key used to locate the given tablets.</param>
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

        private async Task ConnectToClusterAsync(CancellationToken cancellationToken)
        {
            List<HostAndPort> masterAddresses = _options.MasterAddresses;
            var tasks = new HashSet<Task<ConnectToMasterResponse>>(masterAddresses.Count);
            var foundMasters = new List<ServerInfo>(masterAddresses.Count);
            int leaderIndex = -1;

            using var cts = new CancellationTokenSource();
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cts.Token, cancellationToken);

            // Attempt to connect to all configured masters in parallel.
            foreach (var address in masterAddresses)
            {
                var task = ConnectToMasterAsync(address, linkedCts.Token);
                tasks.Add(task);
            }

            while (tasks.Count > 0)
            {
                var task = await Task.WhenAny(tasks).ConfigureAwait(false);
                tasks.Remove(task);

                if (!TryGetConnectResponse(task,
                    out ServerInfo serverInfo,
                    out ConnectToMasterResponsePB responsePb))
                {
                    // Failed to connect to this master.
                    // Failures are fine here, as long as we can
                    // connect to the leader.
                    continue;
                }

                foundMasters.Add(serverInfo);

                if (responsePb.Role == RaftPeerPB.Role.Leader)
                {
                    leaderIndex = foundMasters.Count - 1;

                    _location = responsePb.ClientLocation;

                    var hmsConfig = responsePb.HmsConfig;
                    if (hmsConfig != null)
                    {
                        _hiveMetastoreConfig = new HiveMetastoreConfig(
                            hmsConfig.HmsUris,
                            hmsConfig.HmsSaslEnabled,
                            hmsConfig.HmsUuid);
                    }

                    // Found the leader, that's all we really care about.
                    // Wait a few more seconds to get any followers.
                    cts.CancelAfter(TimeSpan.FromSeconds(3));
                }
            }

            if (leaderIndex == -1)
            {
                throw new NoLeaderFoundException(
                    KuduStatus.ServiceUnavailable("Unable to find the leader master."));
            }

            _masterCache = new ServerInfoCache(foundMasters, leaderIndex);
            _hasConnectedToMaster = true;
        }

        private async Task<ConnectToMasterResponse> ConnectToMasterAsync(
            HostAndPort hostPort, CancellationToken cancellationToken = default)
        {
            ServerInfo serverInfo = await _connectionFactory.GetServerInfoAsync(
                "master", location: null, hostPort).ConfigureAwait(false);

            var rpc = new ConnectToMasterRequest();
            await SendRpcAsync(rpc, serverInfo, cancellationToken).ConfigureAwait(false);

            return new ConnectToMasterResponse(rpc.Response, serverInfo);
        }

        private static bool TryGetConnectResponse(
            Task<ConnectToMasterResponse> task,
            out ServerInfo serverInfo,
            out ConnectToMasterResponsePB responsePb)
        {
            serverInfo = null;
            responsePb = null;

            if (!task.IsCompletedSuccessfully)
            {
                // TODO: Log warning.
                Console.WriteLine("Unable to connect to cluster: " +
                    task.Exception.Message);

                return false;
            }

            ConnectToMasterResponse response = task.Result;

            if (response.ResponsePB.Error != null)
            {
                // TODO: Log warning.
                Console.WriteLine("Error connecting to cluster: " +
                    response.ResponsePB.Error.Status.Message);

                return false;
            }

            serverInfo = response.ServerInfo;
            responsePb = response.ResponsePB;

            return true;
        }

        /// <summary>
        /// Sends the provided <see cref="KuduRpc"/> to the server identified by
        /// RPC's table, partition key, and replica selection.
        /// </summary>
        /// <param name="rpc">The RPC to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        public async Task SendRpcToTabletAsync(
            KuduTabletRpc rpc, CancellationToken cancellationToken = default)
        {
            if (CannotRetryRequest(rpc, cancellationToken))
            {
                // TODO: Change this exception.
                throw new Exception("Attempted Rpc too many times");
            }

            rpc.Attempt++;

            // Set the propagated timestamp so that the next time we send a message to
            // the server the message includes the last propagated timestamp.
            long lastPropagatedTs = LastPropagatedTimestamp;
            if (rpc.ExternalConsistencyMode == ExternalConsistencyMode.ClientPropagated &&
                lastPropagatedTs != NoTimestamp)
            {
                rpc.PropagatedTimestamp = lastPropagatedTs;
            }

            string tableId = rpc.TableId;
            byte[] partitionKey = rpc.PartitionKey;
            RemoteTablet tablet = await GetTabletAsync(tableId, partitionKey).ConfigureAwait(false);
            // TODO: Consider caching non-covered tablet ranges?

            // If we found a tablet, we'll try to find the TS to talk to.
            if (tablet != null)
            {
                ServerInfo serverInfo = GetServerInfo(tablet, rpc.ReplicaSelection);
                if (serverInfo != null)
                {
                    rpc.Tablet = tablet;

                    await SendRpcAsync(rpc, serverInfo, cancellationToken).ConfigureAwait(false);
                    return;
                }
            }

            // We fall through to here in two cases:
            //
            // 1) This client has not yet discovered the tablet which is responsible for
            //    the RPC's table and partition key. This can happen when the client's
            //    tablet location cache is cold because the client is new, or the table
            //    is new.
            //
            // 2) The tablet is known, but we do not have an active client for the
            //    leader replica.

            await SendRpcToTabletAsync(rpc, cancellationToken).ConfigureAwait(false);
        }

        public async Task SendRpcToMasterAsync(
            KuduMasterRpc rpc, CancellationToken cancellationToken = default)
        {
            if (CannotRetryRequest(rpc, cancellationToken))
            {
                // TODO: Change this exception.
                throw new Exception("Attempted Rpc too many times");
            }

            rpc.Attempt++;

            // Set the propagated timestamp so that the next time we send a message to
            // the server the message includes the last propagated timestamp.
            long lastPropagatedTs = LastPropagatedTimestamp;
            if (rpc.ExternalConsistencyMode == ExternalConsistencyMode.ClientPropagated &&
                lastPropagatedTs != NoTimestamp)
            {
                rpc.PropagatedTimestamp = lastPropagatedTs;
            }

            ServerInfo serverInfo = GetMasterServerInfo(rpc.ReplicaSelection);
            if (serverInfo != null)
            {
                await SendRpcAsync(rpc, serverInfo, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await ConnectToClusterAsync(cancellationToken).ConfigureAwait(false);
                await SendRpcToMasterAsync(rpc, cancellationToken).ConfigureAwait(false);
            }
        }

        private ServerInfo GetServerInfo(RemoteTablet tablet, ReplicaSelection replicaSelection)
        {
            return tablet.GetServerInfo(replicaSelection, _location);
        }

        private ServerInfo GetMasterServerInfo(ReplicaSelection replicaSelection)
        {
            return _masterCache?.GetServerInfo(replicaSelection, _location);
        }

        private Task HandleRetryableErrorAsync(
            KuduMasterRpc rpc, KuduException ex, CancellationToken cancellationToken)
        {
            // TODO: we don't always need to sleep, maybe another replica can serve this RPC.
            return DelayedSendRpcAsync(rpc, ex, cancellationToken);
        }

        private Task HandleRetryableErrorAsync(
            KuduTabletRpc rpc, KuduException ex, CancellationToken cancellationToken)
        {
            // TODO: we don't always need to sleep, maybe another replica can serve this RPC.
            return DelayedSendRpcAsync(rpc, ex, cancellationToken);
        }

        /// <summary>
        /// This methods enable putting RPCs on hold for a period of time determined by
        /// <see cref="DelayRpcAsync(KuduRpc, CancellationToken)"/>. If the RPC is out of
        /// time/retries, an exception is thrown.
        /// </summary>
        /// <param name="rpc">The RPC to retry later.</param>
        /// <param name="exception">The reason why we need to retry.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private async Task DelayedSendRpcAsync(
            KuduMasterRpc rpc, KuduException exception, CancellationToken cancellationToken)
        {
            if (CannotRetryRequest(rpc, cancellationToken))
            {
                // Don't let it retry.
                ThrowTooManyAttemptsOrTimeoutException(rpc, exception);
            }

            // Here we simply retry the RPC later. We might be doing this along with a lot of other RPCs
            // in parallel. Asynchbase does some hacking with a "probe" RPC while putting the other ones
            // on hold but we won't be doing this for the moment. Regions in HBase can move a lot,
            // we're not expecting this in Kudu.
            await DelayRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
            await SendRpcToMasterAsync(rpc, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// This methods enable putting RPCs on hold for a period of time determined by
        /// <see cref="DelayRpcAsync(KuduRpc, CancellationToken)"/>. If the RPC is out of
        /// time/retries, an exception is thrown.
        /// </summary>
        /// <param name="rpc">The RPC to retry later.</param>
        /// <param name="exception">The reason why we need to retry.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private async Task DelayedSendRpcAsync(
            KuduTabletRpc rpc, KuduException exception, CancellationToken cancellationToken)
        {
            if (CannotRetryRequest(rpc, cancellationToken))
            {
                // Don't let it retry.
                ThrowTooManyAttemptsOrTimeoutException(rpc, exception);
            }

            // Here we simply retry the RPC later. We might be doing this along with a lot of other RPCs
            // in parallel. Asynchbase does some hacking with a "probe" RPC while putting the other ones
            // on hold but we won't be doing this for the moment. Regions in HBase can move a lot,
            // we're not expecting this in Kudu.
            await DelayRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
            await SendRpcToTabletAsync(rpc, cancellationToken).ConfigureAwait(false);
        }

        private async Task SendRpcAsync(
            KuduMasterRpc rpc, ServerInfo serverInfo,
            CancellationToken cancellationToken = default)
        {
            try
            {
                KuduConnection connection = await _connectionCache.GetConnectionAsync(
                    serverInfo, cancellationToken).ConfigureAwait(false);

                RequestHeader header = CreateRequestHeader(rpc);

                // TODO: Remaining exception handling...
                await connection.SendReceiveAsync(header, rpc).ConfigureAwait(false);
            }
            catch (RecoverableException ex)
            {
                // This is to handle RecoverableException(Status.IllegalState()) from
                // Connection.enqueueMessage() if the connection turned into the TERMINATED state.

                Console.WriteLine("Retrying...");

                await HandleRetryableErrorAsync(rpc, ex, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private async Task SendRpcAsync(
            KuduTabletRpc rpc, ServerInfo serverInfo,
            CancellationToken cancellationToken = default)
        {
            try
            {
                KuduConnection connection = await _connectionCache.GetConnectionAsync(
                    serverInfo, cancellationToken).ConfigureAwait(false);

                RequestHeader header = CreateRequestHeader(rpc);

                // TODO: Remaining exception handling...
                await connection.SendReceiveAsync(header, rpc).ConfigureAwait(false);
            }
            catch (RecoverableException ex)
            {
                // This is to handle RecoverableException(Status.IllegalState()) from
                // Connection.enqueueMessage() if the connection turned into the TERMINATED state.

                Console.WriteLine("Retrying...");

                await HandleRetryableErrorAsync(rpc, ex, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private RequestHeader CreateRequestHeader(KuduRpc rpc)
        {
            // The callId is set by KuduConnection.SendReceiveAsync().
            var header = new RequestHeader
            {
                // TODO: Add required feature flags
                RemoteMethod = new RemoteMethodPB
                {
                    ServiceName = rpc.ServiceName,
                    MethodName = rpc.MethodName
                }
            };

            // Before we create the request, get an authz token if needed. This is done
            // regardless of whether the KuduRpc object already has a token; we may be
            // a retrying due to an invalid token and the client may have a new token.
            if (rpc.NeedsAuthzToken)
            {
                // TODO: Implement this.
                //rpc.AuthzToken = client.GetAuthzToken(rpc.Table.TableId);
            }

            // TODO: Set timeout

            if (rpc.IsRequestTracked)
            {
                if (rpc.SequenceId == RequestTracker.NoSeqNo)
                {
                    rpc.SequenceId = _requestTracker.GetNewSeqNo();
                }

                header.RequestId = new RequestIdPB
                {
                    ClientId = _requestTracker.ClientId,
                    SeqNo = rpc.SequenceId,
                    AttemptNo = rpc.Attempt,
                    FirstIncompleteSeqNo = _requestTracker.FirstIncomplete
                };
            }

            return header;
        }

        private async Task DelayRpcAsync(
            KuduRpc rpc, CancellationToken cancellationToken = default)
        {
            int attemptCount = rpc.Attempt;

            if (attemptCount == 0)
            {
                // If this is the first RPC attempt, don't sleep at all.
                return;
            }

            // Randomized exponential backoff, truncated at 4096ms.
            int sleepTime = (int)(Math.Pow(2.0, Math.Min(attemptCount, 12)) *
                ThreadSafeRandom.Instance.NextDouble());

            await Task.Delay(sleepTime, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Checks whether or not an RPC can be retried once more.
        /// </summary>
        /// <param name="rpc">The RPC we're going to attempt to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private static bool CannotRetryRequest(KuduRpc rpc, CancellationToken cancellationToken)
        {
            return rpc.Attempt > MaxRpcAttempts || cancellationToken.IsCancellationRequested;
        }

        private static void ThrowTooManyAttemptsOrTimeoutException(KuduRpc rpc, KuduException cause)
        {
            string message = rpc.Attempt > MaxRpcAttempts ?
                "Too many attempts." :
                "Request timed out.";

            var statusTimedOut = KuduStatus.TimedOut(message);

            throw new NonRecoverableException(statusTimedOut, cause);
        }

        public static KuduClient Build(string masterAddresses)
        {
            var masters = masterAddresses.Split(',');

            var options = new KuduClientOptions
            {
                MasterAddresses = new List<HostAndPort>(masters.Length)
            };

            foreach (var master in masters)
            {
                var hostPort = EndpointParser.TryParse(master.Trim(), 7051);
                options.MasterAddresses.Add(hostPort);
            }

            return new KuduClient(options);
        }

        private readonly struct ConnectToMasterResponse
        {
            public ConnectToMasterResponsePB ResponsePB { get; }

            public ServerInfo ServerInfo { get; }

            public ConnectToMasterResponse(
                ConnectToMasterResponsePB responsePB, ServerInfo serverInfo)
            {
                ResponsePB = responsePB;
                ServerInfo = serverInfo;
            }
        }
    }
}
