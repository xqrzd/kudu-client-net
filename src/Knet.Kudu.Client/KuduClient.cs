﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Consensus;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Protocol.Rpc;
using Knet.Kudu.Client.Protocol.Security;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Knet.Kudu.Client
{
    public class KuduClient : IAsyncDisposable
    {
        /// <summary>
        /// The number of tablets to fetch from the master in a round trip when
        /// performing a lookup of a single partition (e.g. for a write), or
        /// re-looking-up a tablet with stale information.
        /// </summary>
        private const int FetchTabletsPerPointLookup = 100;
        private const int MaxRpcAttempts = 100;

        public const long NoTimestamp = -1;

        private readonly KuduClientOptions _options;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly ILogger _scanLogger;
        private readonly IKuduConnectionFactory _connectionFactory;
        private readonly SecurityContext _securityContext;
        private readonly ConnectionCache _connectionCache;
        private readonly ConcurrentDictionary<string, TableLocationsCache> _tableLocations;
        private readonly RequestTracker _requestTracker;
        private readonly AuthzTokenCache _authzTokenCache;
        private readonly int _defaultOperationTimeoutMs;

        private volatile bool _hasConnectedToMaster;
        private volatile string _location;
        private volatile ServerInfo _masterLeaderInfo;
        private volatile HiveMetastoreConfig _hiveMetastoreConfig;

        /// <summary>
        /// Timestamp required for HybridTime external consistency through timestamp propagation.
        /// </summary>
        private long _lastPropagatedTimestamp = NoTimestamp;
        private readonly object _lastPropagatedTimestampLock = new object();

        public KuduClient(KuduClientOptions options)
            : this(options, NullLoggerFactory.Instance) { }

        public KuduClient(KuduClientOptions options, ILoggerFactory loggerFactory)
        {
            _options = options;
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<KuduClient>();
            _scanLogger = loggerFactory.CreateLogger("Knet.Kudu.Client.Scanner");
            _securityContext = new SecurityContext();
            _connectionFactory = new KuduConnectionFactory(options, _securityContext, loggerFactory);
            _connectionCache = new ConnectionCache(_connectionFactory, loggerFactory);
            _tableLocations = new ConcurrentDictionary<string, TableLocationsCache>();
            _requestTracker = new RequestTracker(SecurityUtil.NewGuid().ToString("N"));
            _authzTokenCache = new AuthzTokenCache();
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
                var rpc = new ConnectToMasterRequest();
                await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
                return _hiveMetastoreConfig;
            }
        }

        /// <summary>
        /// Export serialized authentication data that may be passed to a different
        /// client instance and imported to provide that client the ability to connect
        /// to the cluster.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        public ValueTask<ReadOnlyMemory<byte>> ExportAuthenticationCredentialsAsync(
            CancellationToken cancellationToken = default)
        {
            if (_hasConnectedToMaster)
            {
                return new ValueTask<ReadOnlyMemory<byte>>(
                    _securityContext.ExportAuthenticationCredentials());
            }

            return ConnectAsync(cancellationToken);

            async ValueTask<ReadOnlyMemory<byte>> ConnectAsync(CancellationToken cancellationToken)
            {
                var rpc = new ConnectToMasterRequest();
                await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
                return _securityContext.ExportAuthenticationCredentials();
            }
        }

        /// <summary>
        /// Import data allowing this client to authenticate to the cluster.
        /// </summary>
        /// <param name="token">
        /// The authentication token provided by a prior call to
        /// <see cref="ExportAuthenticationCredentialsAsync(CancellationToken)"/>.
        /// </param>
        public void ImportAuthenticationCredentials(ReadOnlyMemory<byte> token)
        {
            _securityContext.ImportAuthenticationCredentials(token);
        }

        public async Task<KuduTable> CreateTableAsync(
            TableBuilder table, CancellationToken cancellationToken = default)
        {
            var rpc = new CreateTableRequest(table.Build());
            var response = await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);

            await WaitForTableDoneAsync(response.TableId, cancellationToken).ConfigureAwait(false);

            var tableIdentifier = new TableIdentifierPB { TableId = response.TableId };
            return await OpenTableAsync(tableIdentifier, cancellationToken).ConfigureAwait(false);
        }

        public async Task<AlterTableResponse> AlterTableAsync(
            AlterTableBuilder alterTable,
            CancellationToken cancellationToken = default)
        {
            var rpc = new AlterTableRequest(alterTable);
            AlterTableResponse response;

            try
            {
                response = await SendRpcAsync(rpc, cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                if (alterTable.HasAddDropRangePartitions)
                {
                    // Clear the table locations cache so the new partition is
                    // immediately visible. We clear the cache even on failure,
                    // just in case the alter table operation actually succeeded.
                    _tableLocations.TryRemove(alterTable.TableId, out _);
                }
            }

            if (alterTable.Wait)
            {
                var isDoneResponse = await WaitForIsAlterTableDoneAsync(
                    alterTable.TableIdPb, cancellationToken).ConfigureAwait(false);

                response = new AlterTableResponse(
                    response.TableId,
                    isDoneResponse.SchemaVersion);
            }

            return response;
        }

        private async Task<IsAlterTableDoneResponsePB> WaitForIsAlterTableDoneAsync(
            TableIdentifierPB tableId, CancellationToken cancellationToken)
        {
            var rpc = new IsAlterTableDoneRequest(tableId);

            while (true)
            {
                var response = await SendRpcAsync(rpc, cancellationToken)
                    .ConfigureAwait(false);

                if (response.Done)
                    return response;
                else
                    await Task.Delay(100).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Delete a table on the cluster with the specified name.
        /// </summary>
        /// <param name="tableName">The table's name.</param>
        /// <param name="modifyExternalCatalogs">
        /// Whether to apply the deletion to external catalogs, such as the Hive Metastore.
        /// </param>
        /// <param name="cancellationToken">The cancellation token.</param>
        public async Task DeleteTableAsync(
            string tableName,
            bool modifyExternalCatalogs = true,
            CancellationToken cancellationToken = default)
        {
            var request = new DeleteTableRequestPB
            {
                Table = new TableIdentifierPB { TableName = tableName },
                ModifyExternalCatalogs = modifyExternalCatalogs
            };

            var rpc = new DeleteTableRequest(request);

            await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
        }

        public async Task<List<ListTablesResponsePB.TableInfo>> GetTablesAsync(
            string nameFilter = null, CancellationToken cancellationToken = default)
        {
            var rpc = new ListTablesRequest(nameFilter);
            var response = await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);

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
            var response = await SendRpcAsync(rpc, cancellationToken)
                .ConfigureAwait(false);

            // TODO: Create managed wrapper for this response.
            return response.Servers;
        }

        public Task<List<RemoteTablet>> GetTableLocationsAsync(
            string tableId, byte[] partitionKeyStart, int fetchBatchSize,
            CancellationToken cancellationToken = default)
        {
            return GetTableLocationsAsync(
                tableId, partitionKeyStart, null, fetchBatchSize, cancellationToken);
        }

        public async Task<List<RemoteTablet>> GetTableLocationsAsync(
            string tableId, byte[] partitionKeyStart, byte[] partitionKeyEnd,
            int fetchBatchSize, CancellationToken cancellationToken = default)
        {
            var request = new GetTableLocationsRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId.ToUtf8ByteArray() },
                PartitionKeyStart = partitionKeyStart,
                PartitionKeyEnd = partitionKeyEnd,
                MaxReturnedLocations = (uint)fetchBatchSize,
                InternTsInfosInResponse = true
            };

            var rpc = new GetTableLocationsRequest(request);
            var result = await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);

            var tableLocations = await _connectionFactory.GetTabletsAsync(tableId, result)
                .ConfigureAwait(false);

            return tableLocations;
        }

        public async Task<KuduTable> OpenTableAsync(
            string tableName, CancellationToken cancellationToken = default)
        {
            var tableIdentifier = new TableIdentifierPB { TableName = tableName };
            var response = await GetTableSchemaAsync(tableIdentifier, cancellationToken)
                .ConfigureAwait(false);

            return new KuduTable(response);
        }

        public async Task<WriteResponsePB[]> WriteAsync(
            IEnumerable<KuduOperation> operations,
            ExternalConsistencyMode externalConsistencyMode = ExternalConsistencyMode.ClientPropagated,
            CancellationToken cancellationToken = default)
        {
            var operationsByTablet = new Dictionary<RemoteTablet, List<KuduOperation>>();

            foreach (var operation in operations)
            {
                var tablet = await GetRowTabletAsync(operation, cancellationToken)
                    .ConfigureAwait(false);

                if (!operationsByTablet.TryGetValue(tablet, out var tabletOperations))
                {
                    tabletOperations = new List<KuduOperation>();
                    operationsByTablet.Add(tablet, tabletOperations);
                }

                tabletOperations.Add(operation);
            }

            var tasks = new Task<WriteResponsePB>[operationsByTablet.Count];
            var i = 0;

            foreach (var tabletOperations in operationsByTablet)
            {
                var task = WriteAsync(
                    tabletOperations.Value,
                    tabletOperations.Key,
                    externalConsistencyMode,
                    cancellationToken);

                tasks[i++] = task;
            }

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            // TODO: Save timestamp.
            return results;
        }

        private async Task<WriteResponsePB> WriteAsync(
            List<KuduOperation> operations,
            RemoteTablet tablet,
            ExternalConsistencyMode externalConsistencyMode,
            CancellationToken cancellationToken = default)
        {
            var table = operations[0].Table;

            OperationsEncoder.ComputeSize(
                operations,
                out int rowSize,
                out int indirectSize);

            var rowData = new byte[rowSize];
            var indirectData = new byte[indirectSize];

            OperationsEncoder.Encode(operations, rowData, indirectData);

            var rowOperations = new RowOperationsPB
            {
                Rows = rowData,
                IndirectData = indirectData
            };

            var request = new WriteRequestPB
            {
                TabletId = tablet.TabletId.ToUtf8ByteArray(),
                Schema = table.SchemaPbNoIds.Schema,
                RowOperations = rowOperations,
                ExternalConsistencyMode = (ExternalConsistencyModePB)externalConsistencyMode
            };

            var rpc = new WriteRequest(
                request,
                table.TableId,
                tablet.Partition.PartitionKeyStart);

            var response = await SendRpcAsync(rpc, cancellationToken)
                .ConfigureAwait(false);

            LastPropagatedTimestamp = (long)response.Timestamp;

            return response;
        }

        public KuduScannerBuilder NewScanBuilder(KuduTable table)
        {
            return new KuduScannerBuilder(this, table, _scanLogger);
        }

        public KuduScanTokenBuilder NewScanTokenBuilder(KuduTable table)
        {
            return new KuduScanTokenBuilder(this, table);
        }

        public IKuduSession NewSession() => NewSession(new KuduSessionOptions());

        public IKuduSession NewSession(KuduSessionOptions options)
        {
            return new KuduSession(this, options, _loggerFactory);
        }

        private async Task<KuduTable> OpenTableAsync(
            TableIdentifierPB tableIdentifier, CancellationToken cancellationToken = default)
        {
            var response = await GetTableSchemaAsync(tableIdentifier, cancellationToken)
                .ConfigureAwait(false);

            return new KuduTable(response);
        }

        private Task<GetTableSchemaResponsePB> GetTableSchemaAsync(
            TableIdentifierPB tableIdentifier,
            CancellationToken cancellationToken = default)
        {
            return GetTableSchemaAsync(
                tableIdentifier,
                requiresAuthzTokenSupport: false,
                cancellationToken);
        }

        private async Task<GetTableSchemaResponsePB> GetTableSchemaAsync(
            TableIdentifierPB tableIdentifier,
            bool requiresAuthzTokenSupport,
            CancellationToken cancellationToken = default)
        {
            var request = new GetTableSchemaRequestPB { Table = tableIdentifier };
            var rpc = new GetTableSchemaRequest(request, requiresAuthzTokenSupport);

            var schema = await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);

            var authzToken = schema.AuthzToken;
            if (authzToken != null)
            {
                _authzTokenCache.SetAuthzToken(
                    schema.TableId.ToStringUtf8(), authzToken);
            }

            return schema;
        }

        private async Task WaitForTableDoneAsync(
            byte[] tableId, CancellationToken cancellationToken = default)
        {
            var request = new IsCreateTableDoneRequestPB
            {
                Table = new TableIdentifierPB { TableId = tableId }
            };

            var rpc = new IsCreateTableDoneRequest(request);

            while (true)
            {
                var result = await SendRpcAsync(rpc, cancellationToken)
                    .ConfigureAwait(false);

                if (result.Done)
                    break;

                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                // TODO: Increment rpc attempts.
            }
        }

        internal ValueTask<RemoteTablet> GetRowTabletAsync(
            KuduOperation operation, CancellationToken cancellationToken = default)
        {
            var table = operation.Table;

            int maxSize = KeyEncoder.CalculateMaxPartitionKeySize(
                operation, table.PartitionSchema);
            Span<byte> buffer = stackalloc byte[maxSize];

            KeyEncoder.EncodePartitionKey(
                operation,
                table.PartitionSchema,
                buffer,
                out int bytesWritten);

            var partitionKey = buffer.Slice(0, bytesWritten);

            return GetTabletAsync(table.TableId, partitionKey, cancellationToken);
        }

        /// <summary>
        /// Locates a tablet by consulting the table location cache, then by contacting
        /// a master if we haven't seen the tablet before. The results are cached.
        /// </summary>
        /// <param name="tableId">The table identifier.</param>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
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
            TableLocationsCache cache = GetTableLocationsCache(tableId);
            return cache.FindTablet(partitionKey);
        }

        private bool FindTabletInCache(string tableId, ReadOnlySpan<byte> partitionKey,
            out RemoteTablet left, out RemoteTablet right)
        {
            TableLocationsCache cache = GetTableLocationsCache(tableId);
            return cache.SearchLeftRight(partitionKey, out left, out right);
        }

        /// <summary>
        /// Locates a tablet by consulting a master and caches the results.
        /// </summary>
        /// <param name="tableId">The table identifier.</param>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private async Task<RemoteTablet> LookupAndCacheTabletAsync(
            string tableId, byte[] partitionKey, CancellationToken cancellationToken = default)
        {
            var tablets = await GetTableLocationsAsync(
                tableId,
                partitionKey,
                FetchTabletsPerPointLookup,
                cancellationToken).ConfigureAwait(false);

            CacheTablets(tableId, tablets, partitionKey);

            var tablet = tablets.FindTablet(partitionKey);

            if (tablet == null)
            {
                throw new NonCoveredRangeException(
                    partitionKey, tablets.GetNonCoveredRangeEnd(partitionKey));
            }

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
            TableLocationsCache cache = GetTableLocationsCache(tableId);
            cache.CacheTabletLocations(tablets, partitionKey);
        }

        private void RemoveTabletFromCache(RemoteTablet tablet)
        {
            TableLocationsCache cache = GetTableLocationsCache(tablet.TableId);
            cache.RemoveTablet(tablet.Partition.PartitionKeyStart);
        }

        private TableLocationsCache GetTableLocationsCache(string tableId)
        {
            return _tableLocations.GetOrAdd(tableId, key => new TableLocationsCache());
        }

        public async Task<List<RemoteTablet>> LoopLocateTableAsync(
            KuduTable table,
            byte[] startPartitionKey,
            byte[] endPartitionKey,
            int fetchBatchSize,
            CancellationToken cancellationToken = default)
        {
            // TODO: Use the cache instead of reaching out to a master every time.
            var tablets = await GetTableLocationsAsync(
                table.TableId,
                startPartitionKey,
                endPartitionKey,
                int.MaxValue,
                cancellationToken).ConfigureAwait(false);

            if (tablets.Count == 1)
            {
                var tablet = tablets[0];

                if (startPartitionKey.SequenceCompareTo(tablet.Partition.PartitionKeyStart) > 0)
                    return new List<RemoteTablet>();
            }

            return tablets;
        }

        /// <summary>
        /// Sends a splitKeyRange RPC to split the tablet's primary key range into
        /// smaller ranges. This RPC doesn't change the layout of the tablet.
        /// </summary>
        /// <param name="tableId">Table to lookup.</param>
        /// <param name="startPrimaryKey">
        /// The primary key to begin splitting at (inclusive), pass null to
        /// start splitting at the beginning of the tablet.
        /// </param>
        /// <param name="endPrimaryKey">
        /// The primary key to stop splitting at (exclusive), pass null to
        /// stop splitting at the end of the tablet.
        /// </param>
        /// <param name="partitionKey">The partition key of the tablet to find.</param>
        /// <param name="splitSizeBytes">
        /// The size of the data in each key range. This is a hint: The tablet
        /// server may return a key range larger or smaller than this value.
        /// </param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private Task<List<KeyRangePB>> GetTabletKeyRangesAsync(
            string tableId,
            byte[] startPrimaryKey,
            byte[] endPrimaryKey,
            byte[] partitionKey,
            long splitSizeBytes,
            CancellationToken cancellationToken = default)
        {
            var rpc = new SplitKeyRangeRequest(
                tableId,
                startPrimaryKey,
                endPrimaryKey,
                partitionKey,
                splitSizeBytes);

            return SendRpcAsync(rpc, cancellationToken);
        }

        public async ValueTask<List<KeyRange>> GetTableKeyRangesAsync(
            KuduTable table,
            byte[] startPrimaryKey,
            byte[] endPrimaryKey,
            byte[] startPartitionKey,
            byte[] endPartitionKey,
            int fetchBatchSize,
            long splitSizeBytes,
            CancellationToken cancellationToken = default)
        {
            var tablets = await LoopLocateTableAsync(
                table,
                startPartitionKey,
                endPartitionKey,
                fetchBatchSize,
                cancellationToken).ConfigureAwait(false);

            var keyRanges = new List<KeyRange>(tablets.Count);

            if (splitSizeBytes <= 0)
            {
                foreach (var tablet in tablets)
                {
                    var keyRange = new KeyRange(tablet, startPrimaryKey, endPrimaryKey, -1);
                    keyRanges.Add(keyRange);
                }

                return keyRanges;
            }

            var tasks = new List<Task<SplitKeyRangeResponse>>(tablets.Count);

            foreach (var tablet in tablets)
            {
                var task = GetTabletKeyRangesAsync(this,
                    tablet,
                    startPrimaryKey,
                    endPrimaryKey,
                    splitSizeBytes,
                    cancellationToken);

                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);

            foreach (var result in results)
            {
                foreach (var keyRangePb in result.KeyRanges)
                {
                    var newRange = new KeyRange(
                        result.Tablet,
                        keyRangePb.StartPrimaryKey,
                        keyRangePb.StopPrimaryKey,
                        (long)keyRangePb.SizeBytesEstimates);

                    keyRanges.Add(newRange);
                }
            }

            return keyRanges;

            static async Task<SplitKeyRangeResponse> GetTabletKeyRangesAsync(
                KuduClient client,
                RemoteTablet tablet,
                byte[] startPrimaryKey,
                byte[] endPrimaryKey,
                long splitSizeBytes,
                CancellationToken cancellationToken)
            {
                var keyRanges = await client.GetTabletKeyRangesAsync(
                    tablet.TableId,
                    startPrimaryKey,
                    endPrimaryKey,
                    tablet.Partition.PartitionKeyStart,
                    splitSizeBytes,
                    cancellationToken).ConfigureAwait(false);

                return new SplitKeyRangeResponse(tablet, keyRanges);
            }
        }

        /// <summary>
        /// Return the <see cref="ServerInfo"/> of the current master leader.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        private async Task<ServerInfo> ConnectToClusterAsync(CancellationToken cancellationToken)
        {
            var masterAddresses = _options.MasterAddresses;
            var tasks = new Dictionary<Task<ConnectToMasterResponse>, HostAndPort>();
            var results = new Dictionary<HostAndPort, Exception>();
            ServerInfo leaderServerInfo = null;
            List<HostPortPB> clusterMasterAddresses = null;

            using var cts = new CancellationTokenSource();
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cts.Token, cancellationToken);

            // Attempt to connect to all configured masters in parallel.
            foreach (var address in masterAddresses)
            {
                var task = ConnectToMasterAsync(address, cancellationToken);
                tasks.Add(task, address);
            }

            while (tasks.Count > 0 && leaderServerInfo == null)
            {
                var task = await Task.WhenAny(tasks.Keys).ConfigureAwait(false);
                tasks.Remove(task, out HostAndPort hostPort);

                if (!TryGetConnectResponse(task,
                    out ServerInfo serverInfo,
                    out ConnectToMasterResponsePB responsePb,
                    out Exception exception))
                {
                    // Failed to connect to this master.
                    // Failures are fine here, as long as we can
                    // connect to the leader.

                    results.Add(hostPort, exception);
                    continue;
                }

                clusterMasterAddresses = responsePb.MasterAddrs;

                if (responsePb.Role == RaftPeerPB.Role.Leader)
                {
                    leaderServerInfo = serverInfo;

                    _location = responsePb.ClientLocation;

                    var hmsConfig = responsePb.HmsConfig;
                    if (hmsConfig != null)
                    {
                        _hiveMetastoreConfig = new HiveMetastoreConfig(
                            hmsConfig.HmsUris,
                            hmsConfig.HmsSaslEnabled,
                            hmsConfig.HmsUuid);
                    }

                    var authnToken = responsePb.AuthnToken;
                    if (authnToken != null)
                    {
                        // If the response has security info, adopt it.
                        _securityContext.SetAuthenticationToken(authnToken);
                    }

                    var certificates = responsePb.CaCertDers;
                    if (certificates.Count > 0)
                    {
                        // TODO: Log any exceptions from this.
                        _securityContext.TrustCertificates(certificates);
                    }

                    // Found the leader, that's all we care about.
                    cts.Cancel();
                }
            }

            if (clusterMasterAddresses?.Count > 0 &&
                clusterMasterAddresses.Count != masterAddresses.Count)
            {
                _logger.MisconfiguredMasterAddresses(masterAddresses, clusterMasterAddresses);
            }

            if (leaderServerInfo != null)
            {
                _masterLeaderInfo = leaderServerInfo;
                _hasConnectedToMaster = true;

                return leaderServerInfo;
            }
            else
            {
                var sb = new StringBuilder("Unable to find master leader:");

                foreach (var address in masterAddresses)
                {
                    sb.AppendLine();
                    sb.Append($"\t{address} => ");

                    if (results.TryGetValue(address, out var exception))
                        sb.Append(exception.Message);
                    else
                        sb.Append("Not the leader");
                }

                throw new NoLeaderFoundException(
                    KuduStatus.NetworkError(sb.ToString()),
                    new AggregateException(results.Values));
            }
        }

        private async Task<ConnectToMasterResponse> ConnectToMasterAsync(
            HostAndPort hostPort, CancellationToken cancellationToken)
        {
            ServerInfo serverInfo = await _connectionFactory.GetServerInfoAsync(
                "master", location: null, hostPort).ConfigureAwait(false);

            var rpc = new ConnectToMasterRequest();
            var response = await SendRpcToServerAsync(rpc, serverInfo, cancellationToken)
                .ConfigureAwait(false);

            return new ConnectToMasterResponse(response, serverInfo);
        }

        private bool TryGetConnectResponse(
            Task<ConnectToMasterResponse> task,
            out ServerInfo serverInfo,
            out ConnectToMasterResponsePB responsePb,
            out Exception exception)
        {
            serverInfo = null;
            responsePb = null;

            if (!task.IsCompletedSuccessfully())
            {
                exception = task.Exception.InnerException;
                return false;
            }

            ConnectToMasterResponse response = task.Result;

            if (response.ResponsePB.Error != null)
            {
                var status = KuduStatus.FromMasterErrorPB(response.ResponsePB.Error);
                exception = new RecoverableException(status);
                return false;
            }

            serverInfo = response.ServerInfo;
            responsePb = response.ResponsePB;
            exception = null;

            return true;
        }

        public async Task<T> SendRpcAsync<T>(
            KuduRpc<T> rpc, CancellationToken cancellationToken = default)
        {
            using var cts = new CancellationTokenSource(_defaultOperationTimeoutMs);
            using var linkedCts = CreateLinkedCts(cts, cancellationToken);
            var token = linkedCts.Token;

            try
            {
                while (true)
                {
                    try
                    {
                        if (rpc is KuduMasterRpc<T> masterRpc)
                        {
                            return await SendRpcToMasterInternalAsync(masterRpc, token)
                                .ConfigureAwait(false);
                        }
                        else if (rpc is KuduTabletRpc<T> tabletRpc)
                        {
                            return await SendRpcToTabletInternalAsync(tabletRpc, token)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            throw new NotSupportedException();
                        }
                    }
                    catch (RecoverableException ex)
                    {
                        // Record the exception and retry.
                        HandleRpcException(rpc, ex, token);

                        rpc.Attempt++;
                        await DelayRpcAsync(rpc, token).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                var lastException = rpc.Exception;

                if (lastException == null)
                {
                    // If we haven't recorded any exceptions then this
                    // RPC really did just time out.
                    throw;
                }

                throw new OperationCanceledException(
                    $"Couldn't complete RPC before timeout: {lastException.Message}",
                    lastException);
            }
            finally
            {
                CompleteTrackedRpc(rpc);
            }
        }

        private static CancellationTokenSource CreateLinkedCts(
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

        private void CompleteTrackedRpc(KuduRpc rpc)
        {
            // If this is a "tracked RPC" unregister it, unless it never
            // got to the point of being registered.
            if (rpc.IsRequestTracked)
            {
                long sequenceId = rpc.SequenceId;

                if (sequenceId != RequestTracker.NoSeqNo)
                {
                    _requestTracker.CompleteRpc(sequenceId);
                }
            }
        }

        private async Task<T> SendRpcToMasterInternalAsync<T>(
            KuduMasterRpc<T> rpc, CancellationToken cancellationToken)
        {
            ServerInfo serverInfo = _masterLeaderInfo;

            if (serverInfo == null)
            {
                serverInfo = await ConnectToClusterAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            return await SendRpcToServerAsync(rpc, serverInfo, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Sends the provided <see cref="KuduTabletRpc{T}"/> to the server
        /// identified by RPC's table, partition key, and replica selection.
        /// </summary>
        /// <param name="rpc">The RPC to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private async Task<T> SendRpcToTabletInternalAsync<T>(
            KuduTabletRpc<T> rpc, CancellationToken cancellationToken)
        {
            // Set the propagated timestamp so that the next time we send a message to
            // the server the message includes the last propagated timestamp.
            long lastPropagatedTimestamp = LastPropagatedTimestamp;
            if (rpc.ExternalConsistencyMode == ExternalConsistencyMode.ClientPropagated &&
                lastPropagatedTimestamp != NoTimestamp)
            {
                rpc.PropagatedTimestamp = lastPropagatedTimestamp;
            }

            string tableId = rpc.TableId;

            // Before we create the request, get an authz token if needed. This is done
            // regardless of whether the KuduRpc object already has a token; we may be
            // a retrying due to an invalid token and the client may have a new token.
            if (rpc.NeedsAuthzToken)
            {
                rpc.AuthzToken = await GetAuthzTokenAsync(tableId, cancellationToken)
                    .ConfigureAwait(false);
            }

            RemoteTablet tablet = await GetTabletAsync(
                tableId,
                rpc.PartitionKey,
                cancellationToken).ConfigureAwait(false);

            rpc.Tablet = tablet;

            ServerInfo serverInfo = GetServerInfo(tablet, rpc.ReplicaSelection);

            if (serverInfo == null)
            {
                RemoveTabletFromCache(tablet);

                throw new RecoverableException(KuduStatus.IllegalState(
                    $"Unable to find {rpc.ReplicaSelection} replica in {tablet}"));
            }

            return await SendRpcToServerAsync(rpc, serverInfo, cancellationToken)
                .ConfigureAwait(false);
        }

        private ServerInfo GetServerInfo(RemoteTablet tablet, ReplicaSelection replicaSelection)
        {
            return tablet.GetServerInfo(replicaSelection, _location);
        }

        private async ValueTask<SignedTokenPB> GetAuthzTokenAsync(
            string tableId, CancellationToken cancellationToken)
        {
            var authzToken = _authzTokenCache.GetAuthzToken(tableId);
            if (authzToken == null)
            {
                var tableIdPb = new TableIdentifierPB
                {
                    TableId = tableId.ToUtf8ByteArray()
                };

                // This call will also cache the authz token.
                var schema = await GetTableSchemaAsync(
                    tableIdPb,
                    requiresAuthzTokenSupport: true,
                    cancellationToken).ConfigureAwait(false);

                authzToken = schema.AuthzToken;

                if (authzToken == null)
                {
                    throw new NonRecoverableException(KuduStatus.InvalidArgument(
                        $"No authz token retrieved for {tableId}"));
                }
            }

            return authzToken;
        }

        internal async ValueTask<HostAndPort> FindLeaderMasterServerAsync(
            CancellationToken cancellationToken = default)
        {
            // Consult the cache to determine the current leader master.
            //
            // If one isn't found, issue an RPC that retries until the leader master
            // is discovered. We don't need the RPC's results; it's just a simple way to
            // wait until a leader master is elected.

            var serverInfo = _masterLeaderInfo;
            if (serverInfo == null)
            {
                // If there's no leader master, this will time out and throw an exception.
                await GetTabletServersAsync(cancellationToken).ConfigureAwait(false);

                serverInfo = _masterLeaderInfo;
                if (serverInfo == null)
                {
                    throw new NonRecoverableException(KuduStatus.IllegalState(
                        "Master leader could not be found"));
                }
            }

            return serverInfo.HostPort;
        }

        private async Task<T> SendRpcToServerAsync<T>(
            KuduMasterRpc<T> rpc,
            ServerInfo serverInfo,
            CancellationToken cancellationToken)
        {
            await SendRpcGenericAsync(rpc, serverInfo, cancellationToken)
                .ConfigureAwait(false);

            if (rpc.Error != null)
            {
                var code = rpc.Error.Status.Code;
                var status = KuduStatus.FromMasterErrorPB(rpc.Error);

                if (rpc.Error.code == MasterErrorPB.Code.NotTheLeader)
                {
                    InvalidateMasterServerCache();
                    throw new RecoverableException(status);
                }
                else if (code == AppStatusPB.ErrorCode.ServiceUnavailable)
                {
                    throw new RecoverableException(status);
                }
                else
                {
                    throw new NonRecoverableException(status);
                }
            }

            return rpc.Output;
        }

        private async Task<T> SendRpcToServerAsync<T>(
            KuduTabletRpc<T> rpc,
            ServerInfo serverInfo,
            CancellationToken cancellationToken)
        {
            await SendRpcGenericAsync(rpc, serverInfo, cancellationToken)
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
                else if (
                    errStatusCode == AppStatusPB.ErrorCode.IllegalState ||
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

            return rpc.Output;
        }

        private async Task SendRpcGenericAsync<T>(
            KuduRpc<T> rpc, ServerInfo serverInfo,
            CancellationToken cancellationToken = default)
        {
            RequestHeader header = CreateRequestHeader(rpc);
            KuduConnection connection = null;

            try
            {
                connection = await _connectionCache.GetConnectionAsync(
                    serverInfo, cancellationToken).ConfigureAwait(false);

                await connection.SendReceiveAsync(header, rpc, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (InvalidAuthnTokenException)
            {
                // This connection can't be used anymore.
                // Close the connection and rethrow the exception so we
                // can retry in the hopes the user imported a new token.

                // TODO: Here we just want to invoke the disconnected callback,
                // and close just the writing side of the pipe (leave the read
                // side open to allow pending RPCs to finish, instead of forcing
                // them to retry on a new connection).
                if (connection != null)
                    await connection.StopAsync().ConfigureAwait(false);

                throw;
            }
            catch (InvalidAuthzTokenException)
            {
                RemoveCachedAuthzToken(rpc);
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
                        InvalidateMasterServerCache();
                    }
                }

                throw;
            }
        }

        private void InvalidateMasterServerCache()
        {
            _masterLeaderInfo = null;
        }

        private void RemoveCachedAuthzToken<T>(KuduRpc<T> rpc)
        {
            if (rpc is KuduTabletRpc<T> tabletRpc)
            {
                var tableId = tabletRpc.TableId;
                if (tableId == null)
                {
                    throw new NonRecoverableException(
                        KuduStatus.InvalidArgument("Rpc did not set TableId"));
                }

                _authzTokenCache.RemoveAuthzToken(tableId);
            }
            else
            {
                throw new NonRecoverableException(KuduStatus.InvalidArgument(
                    "Expected InvalidAuthzTokenException on tablet RPCs"));
            }
        }

        private RequestHeader CreateRequestHeader(KuduRpc rpc)
        {
            // CallId is set by KuduConnection.SendReceiveAsync().
            var header = new RequestHeader
            {
                RemoteMethod = new RemoteMethodPB
                {
                    ServiceName = rpc.ServiceName,
                    MethodName = rpc.MethodName
                },
                RequiredFeatureFlags = rpc.RequiredFeatures,
                TimeoutMillis = (uint)_defaultOperationTimeoutMs
            };

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

        private static Task DelayRpcAsync(KuduRpc rpc, CancellationToken cancellationToken)
        {
            int attemptCount = rpc.Attempt;

            // Randomized exponential backoff, truncated at 4096ms.
            int sleepTime = (int)(Math.Pow(2.0, Math.Min(attemptCount, 12)) *
                ThreadSafeRandom.Instance.NextDouble());

            return Task.Delay(sleepTime, cancellationToken);
        }

        private void HandleRpcException(
            KuduRpc rpc, Exception exception, CancellationToken cancellationToken)
        {
            _logger.RecoverableRpcException(exception);

            // Record the last exception, so if the operation times out we can report it.
            rpc.Exception = exception;

            var numAttempts = rpc.Attempt;
            if (numAttempts > MaxRpcAttempts)
            {
                throw new NonRecoverableException(
                    KuduStatus.TimedOut($"Too many RPC attempts: {numAttempts}"),
                    exception);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(
                    $"Couldn't complete RPC before timeout: {exception.Message}",
                    exception);
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

        private readonly struct SplitKeyRangeResponse
        {
            public RemoteTablet Tablet { get; }

            public List<KeyRangePB> KeyRanges { get; }

            public SplitKeyRangeResponse(
                RemoteTablet tablet,
                List<KeyRangePB> keyRanges)
            {
                Tablet = tablet;
                KeyRanges = keyRanges;
            }
        }
    }
}
