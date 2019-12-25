using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Builder;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Consensus;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Protocol.Rpc;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
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

        private readonly KuduClientOptions _options;
        private readonly IKuduConnectionFactory _connectionFactory;
        private readonly ConnectionCache _connectionCache;
        private readonly Dictionary<string, TableLocationsCache> _tableLocations;
        private readonly RequestTracker _requestTracker;
        private readonly int _defaultOperationTimeoutMs;

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
            _connectionFactory = new KuduConnectionFactory(options);
            _connectionCache = new ConnectionCache(_connectionFactory);
            _tableLocations = new Dictionary<string, TableLocationsCache>();
            _requestTracker = new RequestTracker(Guid.NewGuid().ToString("N"));
            _defaultOperationTimeoutMs = (int)options.DefaultOperationTimeout.TotalMilliseconds;
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
                var rpc = new ConnectToMasterRequest2();
                await SendRpcToMasterAsync(rpc, cancellationToken).ConfigureAwait(false);
                return _hiveMetastoreConfig;
            }
        }

        public async Task<KuduTable> CreateTableAsync(TableBuilder table)
        {
            var rpc = new CreateTableRequest(table.Build());
            var response = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

            await WaitForTableDoneAsync(response.TableId).ConfigureAwait(false);

            var tableIdentifier = new TableIdentifierPB { TableId = response.TableId };
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
            var request = new DeleteTableRequestPB
            {
                Table = new TableIdentifierPB { TableName = tableName },
                ModifyExternalCatalogs = modifyExternalCatalogs
            };

            var rpc = new DeleteTableRequest(request);

            await SendRpcToMasterAsync(rpc).ConfigureAwait(false);
        }

        public async Task<List<ListTablesResponsePB.TableInfo>> GetTablesAsync(string nameFilter = null)
        {
            var rpc = new ListTablesRequest(nameFilter);
            var response = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

            return response.Tables;
        }

        /// <summary>
        /// Get the list of running tablet servers.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        public async Task<List<ListTabletServersResponsePB.Entry>> GetTabletServersAsync(
            CancellationToken cancellationToken = default)
        {
            var rpc = new ListTabletServersRequest();
            var response = await SendRpcToMasterAsync(rpc, cancellationToken)
                .ConfigureAwait(false);

            // TODO: Create managed wrapper for this response.
            return response.Servers;
        }

        public async Task<List<RemoteTablet>> GetTableLocationsAsync(
            string tableId, byte[] partitionKey, uint fetchBatchSize,
            CancellationToken cancellationToken = default)
        {
            // TODO: rate-limit master lookups.

            var request = new GetTableLocationsRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId.ToUtf8ByteArray() },
                PartitionKeyStart = partitionKey,
                MaxReturnedLocations = fetchBatchSize
            };

            var rpc = new GetTableLocationsRequest(request);
            var result = await SendRpcToMasterAsync(rpc, cancellationToken).ConfigureAwait(false);

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

        public async Task<WriteResponsePB[]> WriteRowAsync(
            IEnumerable<Operation> operations,
            ExternalConsistencyMode externalConsistencyMode = ExternalConsistencyMode.ClientPropagated)
        {
            var operationsByTablet = new Dictionary<RemoteTablet, List<Operation>>();

            foreach (var operation in operations)
            {
                var tablet = await GetRowTabletAsync(operation).ConfigureAwait(false);

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
                var task = WriteRowAsync(
                    tabletOperations.Value,
                    tabletOperations.Key,
                    externalConsistencyMode);

                tasks[i++] = task;
            }

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            // TODO: Save timestamp.
            return results;
        }

        private async Task<WriteResponsePB> WriteRowAsync(
            List<Operation> operations,
            RemoteTablet tablet,
            ExternalConsistencyMode externalConsistencyMode)
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

            var rowOperations = new RowOperationsPB
            {
                Rows = rowData,
                IndirectData = indirectData
            };

            var request = new WriteRequestPB
            {
                TabletId = tablet.TabletId.ToUtf8ByteArray(),
                Schema = table.SchemaPb.Schema,
                RowOperations = rowOperations,
                ExternalConsistencyMode = (ExternalConsistencyModePB)externalConsistencyMode
            };

            long lastPropagatedTimestamp = LastPropagatedTimestamp;
            if (lastPropagatedTimestamp != NoTimestamp)
            {
                // TODO: This could be different from the one set by SendRpcToTabletAsync()
                request.PropagatedTimestamp = (ulong)lastPropagatedTimestamp;
            }

            var rpc = new WriteRequest(
                request,
                table.TableId,
                tablet.Partition.PartitionKeyStart);

            return await SendRpcToTabletAsync(rpc).ConfigureAwait(false);
        }

        public ScanBuilder NewScanBuilder(KuduTable table)
        {
            return new ScanBuilder(this, table);
        }

        public IKuduSession NewSession(KuduSessionOptions options)
        {
            var session = new KuduSession(this, options);
            session.StartProcessing();
            return session;
        }

        private async Task<KuduTable> OpenTableAsync(TableIdentifierPB tableIdentifier)
        {
            var response = await GetTableSchemaAsync(tableIdentifier).ConfigureAwait(false);

            return new KuduTable(response);
        }

        private async Task<GetTableSchemaResponsePB> GetTableSchemaAsync(TableIdentifierPB tableIdentifier)
        {
            var request = new GetTableSchemaRequestPB { Table = tableIdentifier };
            var rpc = new GetTableSchemaRequest(request);

            return await SendRpcToMasterAsync(rpc).ConfigureAwait(false);
        }

        private async Task WaitForTableDoneAsync(byte[] tableId)
        {
            var request = new IsCreateTableDoneRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId }
            };

            var rpc = new IsCreateTableDoneRequest(request);

            while (true)
            {
                var result = await SendRpcToMasterAsync(rpc).ConfigureAwait(false);

                if (result.Done)
                    break;

                await Task.Delay(50).ConfigureAwait(false);
                // TODO: Increment rpc attempts.
            }
        }

        internal ValueTask<RemoteTablet> GetRowTabletAsync(Operation operation)
        {
            var table = operation.Table;
            var row = operation.Row;

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

        private void RemoveTabletFromCache(RemoteTablet tablet)
        {
            TableLocationsCache cache;

            lock (_tableLocations)
            {
                if (!_tableLocations.TryGetValue(tablet.TableId, out cache))
                    return;
            }

            cache.RemoveTablet(tablet.Partition.PartitionKeyStart);
        }

        private async Task ConnectToClusterAsync(RequestTimeoutTracker timeoutTracker)
        {
            var masterAddresses = _options.MasterAddresses;
            var tasks = new HashSet<Task<ConnectToMasterResponse>>();
            var foundMasters = new List<ServerInfo>(masterAddresses.Count);
            int leaderIndex = -1;

            using var cts = new CancellationTokenSource();
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cts.Token, timeoutTracker);

            // Attempt to connect to all configured masters in parallel.
            foreach (var address in masterAddresses)
            {
                var nTimeoutTracker = new RequestTimeoutTracker(linkedCts.Token);
                var task = ConnectToMasterAsync(address, nTimeoutTracker);
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

            if (leaderIndex != -1)
            {
                _masterCache = new ServerInfoCache(foundMasters, leaderIndex);
                _hasConnectedToMaster = true;
            }
            else
            {
                await DelayRpcAsync(timeoutTracker).ConfigureAwait(false);
            }
        }

        private async Task<ConnectToMasterResponse> ConnectToMasterAsync(
            HostAndPort hostPort, RequestTimeoutTracker timeoutTracker)
        {
            ServerInfo serverInfo = await _connectionFactory.GetServerInfoAsync(
                "master", location: null, hostPort).ConfigureAwait(false);

            var rpc = new ConnectToMasterRequest();
            var response = await SendRpcToServerAsync(rpc, serverInfo, timeoutTracker)
                .ConfigureAwait(false);

            return new ConnectToMasterResponse(response, serverInfo);
        }

        private static bool TryGetConnectResponse(
            Task<ConnectToMasterResponse> task,
            out ServerInfo serverInfo,
            out ConnectToMasterResponsePB responsePb)
        {
            serverInfo = null;
            responsePb = null;

            if (!task.IsCompletedSuccessfully())
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

        public async Task<T> SendRpcToMasterAsync<T>(
            KuduMasterRpc<T> rpc, CancellationToken cancellationToken = default)
        {
            using var cts = new CancellationTokenSource(_defaultOperationTimeoutMs);
            using var linkedCts = CreateLinkedCts(cts, cancellationToken);

            var timeoutTracker = new RequestTimeoutTracker(linkedCts.Token);

            return await SendRpcToMasterAsync(rpc, timeoutTracker).ConfigureAwait(false);
        }

        public async Task<T> SendRpcToTabletAsync<T>(
            KuduTabletRpc<T> rpc, CancellationToken cancellationToken = default)
        {
            using var cts = new CancellationTokenSource(_defaultOperationTimeoutMs);
            using var linkedCts = CreateLinkedCts(cts, cancellationToken);

            var timeoutTracker = new RequestTimeoutTracker(linkedCts.Token);

            return await SendRpcToTabletAsync(rpc, timeoutTracker).ConfigureAwait(false);
        }

        private CancellationTokenSource CreateLinkedCts(
            CancellationTokenSource timeout,
            CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled)
            {
                return CancellationTokenSource.CreateLinkedTokenSource(
                    timeout.Token, cancellationToken);
            }

            return timeout;
        }

        private async Task<T> SendRpcToMasterAsync<T>(
            KuduMasterRpc<T> rpc, RequestTimeoutTracker timeoutTracker)
        {
            ThrowIfRpcTimedOut(timeoutTracker);

            rpc.Attempt++;
            timeoutTracker.NumAttempts++;

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
                return await SendRpcToServerAsync(rpc, serverInfo, timeoutTracker).ConfigureAwait(false);
            }
            else
            {
                await ConnectToClusterAsync(timeoutTracker).ConfigureAwait(false);
                return await SendRpcToMasterAsync(rpc, timeoutTracker).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Sends the provided <see cref="KuduTabletRpc{T}"/> to the server
        /// identified by RPC's table, partition key, and replica selection.
        /// </summary>
        /// <param name="rpc">The RPC to send.</param>
        /// <param name="timeoutTracker">The timeout tracker.</param>
        private async Task<T> SendRpcToTabletAsync<T>(
            KuduTabletRpc<T> rpc, RequestTimeoutTracker timeoutTracker)
        {
            ThrowIfRpcTimedOut(timeoutTracker);

            rpc.Attempt++;
            timeoutTracker.NumAttempts++;

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
            RemoteTablet tablet = await GetTabletAsync(
                tableId, partitionKey, timeoutTracker).ConfigureAwait(false);

            // If we found a tablet, we'll try to find the TS to talk to.
            if (tablet != null)
            {
                ServerInfo serverInfo = GetServerInfo(tablet, rpc.ReplicaSelection);
                if (serverInfo != null)
                {
                    rpc.Tablet = tablet;

                    return await SendRpcToServerAsync(rpc, serverInfo, timeoutTracker)
                        .ConfigureAwait(false);
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

            return await SendRpcToTabletAsync(rpc, timeoutTracker).ConfigureAwait(false);
        }

        private ServerInfo GetServerInfo(RemoteTablet tablet, ReplicaSelection replicaSelection)
        {
            return tablet.GetServerInfo(replicaSelection, _location);
        }

        private ServerInfo GetMasterServerInfo(ReplicaSelection replicaSelection)
        {
            return _masterCache?.GetServerInfo(replicaSelection, _location);
        }

        internal async ValueTask<HostAndPort> FindLeaderMasterServerAsync(
            CancellationToken cancellationToken = default)
        {
            // Consult the cache to determine the current leader master.
            //
            // If one isn't found, issue an RPC that retries until the leader master
            // is discovered. We don't need the RPC's results; it's just a simple way to
            // wait until a leader master is elected.

            var serverInfo = GetMasterServerInfo(ReplicaSelection.LeaderOnly);
            if (serverInfo == null)
            {
                // If there's no leader master, this will time out and throw an exception.
                await GetTabletServersAsync(cancellationToken).ConfigureAwait(false);

                serverInfo = GetMasterServerInfo(ReplicaSelection.LeaderOnly);
                if (serverInfo == null)
                {
                    throw new NonRecoverableException(KuduStatus.IllegalState(
                        "Master leader could not be found"));
                }
            }

            return serverInfo.HostPort;
        }

        private Task<T> HandleRetryableErrorAsync<T>(
            KuduRpc<T> rpc, KuduException ex, RequestTimeoutTracker timeoutTracker)
        {
            // TODO: we don't always need to sleep, maybe another replica can serve this RPC.
            return DelayedSendRpcAsync(rpc, ex, timeoutTracker);
        }

        /// <summary>
        /// This methods enable putting RPCs on hold for a period of time determined by
        /// <see cref="DelayRpcAsync(RequestTimeoutTracker)"/>. If the RPC is
        /// out of time/retries, an exception is thrown.
        /// </summary>
        /// <param name="rpc">The RPC to retry later.</param>
        /// <param name="exception">The reason why we need to retry.</param>
        /// <param name="timeoutTracker">The timeout tracker.</param>
        private async Task<T> DelayedSendRpcAsync<T>(
            KuduRpc<T> rpc, KuduException exception, RequestTimeoutTracker timeoutTracker)
        {
            // TODO:
            //if (CannotRetryRequest(timeoutTracker))
            //{
            //    // Don't let it retry.
            //    ThrowTooManyAttemptsOrTimeoutException(rpc, exception);
            //}

            // Here we simply retry the RPC later. We might be doing this along with a
            // lot of other RPCs in parallel. Asynchbase does some hacking with a "probe"
            // RPC while putting the other ones on hold but we won't be doing this for the
            // moment. Regions in HBase can move a lot, we're not expecting this in Kudu.
            await DelayRpcAsync(timeoutTracker).ConfigureAwait(false);

            if (rpc is KuduTabletRpc<T> tabletRpc)
            {
                return await SendRpcToTabletAsync(tabletRpc, timeoutTracker)
                    .ConfigureAwait(false);
            }
            else if (rpc is KuduMasterRpc<T> masterRpc)
            {
                return await SendRpcToMasterAsync(masterRpc, timeoutTracker)
                    .ConfigureAwait(false);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private async Task<T> SendRpcToServerAsync<T>(
            KuduMasterRpc<T> rpc,
            ServerInfo serverInfo,
            RequestTimeoutTracker timeoutTracker)
        {
            try
            {
                var result = await SendRpcGenericAsync(rpc, serverInfo, timeoutTracker)
                    .ConfigureAwait(false);

                if (rpc.Error != null)
                {
                    var code = rpc.Error.Status.Code;
                    var status = KuduStatus.FromMasterErrorPB(rpc.Error);
                    if (rpc.Error.code == MasterErrorPB.Code.NotTheLeader)
                    {
                        _masterCache = null;
                        throw new RecoverableException(status);
                    }
                    else if (code == AppStatusPB.ErrorCode.ServiceUnavailable)
                    {
                        // TODO: This is a crutch until we either don't have to retry
                        // RPCs going to the same server or use retry policies.
                        throw new RecoverableException(status);
                    }
                    else
                    {
                        throw new NonRecoverableException(status);
                    }
                }

                return result;
            }
            catch (RecoverableException ex)
            {
                if (rpc is ConnectToMasterRequest)
                {
                    // Special case:
                    // We never want to retry this RPC, we only use it to poke masters to
                    // learn where the leader is. If the error is truly non recoverable,
                    // it'll be handled later.
                    throw;
                }

                return await HandleRetryableErrorAsync(rpc, ex, timeoutTracker)
                    .ConfigureAwait(false);
            }
        }

        private async Task<T> SendRpcToServerAsync<T>(
            KuduTabletRpc<T> rpc,
            ServerInfo serverInfo,
            RequestTimeoutTracker timeoutTracker)
        {
            try
            {
                var result = await SendRpcGenericAsync(rpc, serverInfo, timeoutTracker)
                    .ConfigureAwait(false);

                if (rpc.Error != null)
                {
                    var errCode = rpc.Error.code;
                    var errStatusCode = rpc.Error.Status.Code;
                    var status = KuduStatus.FromTabletServerErrorPB(rpc.Error);

                    if (errCode == TabletServerErrorPB.Code.TabletNotFound)
                    {
                        // We're handling a tablet server that's telling us it doesn't
                        // have the tablet we're asking for.
                        RemoveTabletFromCache(rpc.Tablet);
                        throw new RecoverableException(status);
                    }
                    else if (errCode == TabletServerErrorPB.Code.TabletNotRunning ||
                        errStatusCode == AppStatusPB.ErrorCode.ServiceUnavailable)
                    {
                        throw new RecoverableException(status);
                    }
                    else if (errStatusCode == AppStatusPB.ErrorCode.IllegalState ||
                        errStatusCode == AppStatusPB.ErrorCode.Aborted)
                    {
                        // These two error codes are an indication that the tablet
                        // isn't a leader.
                        RemoveTabletFromCache(rpc.Tablet);
                        throw new RecoverableException(status);
                    }
                    else
                    {
                        throw new NonRecoverableException(status);
                    }
                }

                return result;
            }
            catch (RecoverableException ex)
            {
                return await HandleRetryableErrorAsync(rpc, ex, timeoutTracker)
                    .ConfigureAwait(false);
            }
        }

        private async Task<T> SendRpcGenericAsync<T>(
            KuduRpc<T> rpc, ServerInfo serverInfo,
            CancellationToken cancellationToken = default)
        {
            RequestHeader header = CreateRequestHeader(rpc);

            try
            {
                KuduConnection connection = await _connectionCache.GetConnectionAsync(
                    serverInfo, cancellationToken).ConfigureAwait(false);

                return await connection.SendReceiveAsync(header, rpc)
                    .ConfigureAwait(false);
            }
            catch (InvalidAuthnTokenException)
            {
                // TODO
                Console.WriteLine("HandleInvalidAuthnToken");
                throw;
            }
            catch (InvalidAuthzTokenException)
            {
                // TODO
                Console.WriteLine("HandleInvalidAuthzToken");
                throw;
            }
            catch (RecoverableException ex)
            {
                if (!ex.Status.IsServiceUnavailable)
                {
                    // If we don't really know anything about the exception, invalidate
                    // the location for the tablet, opening the possibility of retrying
                    // on a different server.

                    if (rpc is KuduTabletRpc<T> tabletRpc)
                    {
                        RemoveTabletFromCache(tabletRpc.Tablet);
                    }
                    else if (rpc is KuduMasterRpc<T>)
                    {
                        _masterCache = null;
                    }
                }

                throw;
            }
        }

        private RequestHeader CreateRequestHeader<T>(KuduRpc<T> rpc)
        {
            // The callId is set by KuduConnection.SendReceiveAsync().
            var header = new RequestHeader
            {
                // TODO: Add required feature flags
                RemoteMethod = new RemoteMethodPB
                {
                    ServiceName = rpc.ServiceName,
                    MethodName = rpc.MethodName
                },
                TimeoutMillis = (uint)_defaultOperationTimeoutMs
            };

            // Before we create the request, get an authz token if needed. This is done
            // regardless of whether the KuduRpc object already has a token; we may be
            // a retrying due to an invalid token and the client may have a new token.
            if (rpc.NeedsAuthzToken)
            {
                // TODO: Implement this.
                //rpc.AuthzToken = client.GetAuthzToken(rpc.Table.TableId);
            }

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

        private async Task DelayRpcAsync(RequestTimeoutTracker timeoutTracker)
        {
            int attemptCount = timeoutTracker.NumAttempts;

            if (attemptCount == 0)
            {
                // If this is the first RPC attempt, don't sleep at all.
                return;
            }

            // Randomized exponential backoff, truncated at 4096ms.
            int sleepTime = (int)(Math.Pow(2.0, Math.Min(attemptCount, 12)) *
                ThreadSafeRandom.Instance.NextDouble());

            await Task.Delay(sleepTime, timeoutTracker).ConfigureAwait(false);
        }

        private static void ThrowIfRpcTimedOut(RequestTimeoutTracker timeoutTracker)
        {
            timeoutTracker.CancellationToken.ThrowIfCancellationRequested();

            var numAttempts = timeoutTracker.NumAttempts;
            if (numAttempts > MaxRpcAttempts)
            {
                throw new NonRecoverableException(
                    KuduStatus.TimedOut($"Too many RPC attempts: {numAttempts}"));
            }
        }

        public static KuduClientBuilder NewBuilder(string masterAddresses)
        {
            return new KuduClientBuilder(masterAddresses);
        }

        public static KuduClientBuilder NewBuilder(IReadOnlyList<HostAndPort> masterAddresses)
        {
            return new KuduClientBuilder(masterAddresses);
        }

        private readonly struct ConnectToMasterResponse
        {
            public ConnectToMasterResponsePB ResponsePB { get; }

            public ServerInfo ServerInfo { get; }

            public ConnectToMasterResponse(
                ConnectToMasterResponsePB responsePB,
                ServerInfo serverInfo)
            {
                ResponsePB = responsePB;
                ServerInfo = serverInfo;
            }
        }

        private class RequestTimeoutTracker
        {
            public CancellationToken CancellationToken;

            /// <summary>
            /// Total number of 'external call attempts'. This
            /// includes operations done on behalf of this RPC,
            /// such as looking up tablet locations.
            /// </summary>
            public int NumAttempts;

            public RequestTimeoutTracker(CancellationToken cancellationToken, int numAttempts = 0)
            {
                CancellationToken = cancellationToken;
                NumAttempts = numAttempts;
            }

            public static implicit operator CancellationToken(RequestTimeoutTracker t) => t.CancellationToken;
        }
    }
}
