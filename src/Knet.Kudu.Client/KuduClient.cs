using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Client;
using Knet.Kudu.Client.Protobuf.Consensus;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protobuf.Rpc;
using Knet.Kudu.Client.Protobuf.Security;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client;

public sealed class KuduClient : IAsyncDisposable
{
    /// <summary>
    /// The number of tablets to fetch from the master in a round trip when
    /// performing a lookup of a single partition (e.g. for a write), or
    /// re-looking-up a tablet with stale information.
    /// </summary>
    private const int FetchTabletsPerPointLookup = 100;
    private const int MaxRpcAttempts = 100;

    public const long NoTimestamp = -1;
    public const long InvalidTxnId = -1;

    internal static readonly KuduSessionOptions DefaultSessionOptions = new();

    private readonly KuduClientOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly ILogger _scanLogger;
    private readonly IKuduConnectionFactory _connectionFactory;
    private readonly ISystemClock _systemClock;
    private readonly ISecurityContext _securityContext;
    private readonly ConnectionCache _connectionCache;
    private readonly ConcurrentDictionary<string, TableLocationsCache> _tableLocations;
    private readonly RequestTracker _requestTracker;
    private readonly AuthzTokenCache _authzTokenCache;
    private readonly SemaphoreSlim _singleClusterConnect;
    private readonly int _defaultOperationTimeoutMs;

    private volatile MasterLeaderInfo _masterLeaderInfo;

    /// <summary>
    /// Timestamp required for HybridTime external consistency through timestamp propagation.
    /// </summary>
    private long _lastPropagatedTimestamp = NoTimestamp;
    private readonly object _lastPropagatedTimestampLock = new();

    public KuduClient(
        KuduClientOptions options,
        ISecurityContext securityContext,
        IKuduConnectionFactory connectionFactory,
        ISystemClock systemClock,
        ILoggerFactory loggerFactory)
    {
        _options = options;
        _securityContext = securityContext;
        _connectionFactory = connectionFactory;
        _systemClock = systemClock;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<KuduClient>();
        _scanLogger = loggerFactory.CreateLogger("Knet.Kudu.Client.Scanner");
        _connectionCache = new ConnectionCache(_connectionFactory, loggerFactory);
        _tableLocations = new ConcurrentDictionary<string, TableLocationsCache>();
        _requestTracker = new RequestTracker(Guid.NewGuid().ToString("N"));
        _authzTokenCache = new AuthzTokenCache();
        _singleClusterConnect = new SemaphoreSlim(1, 1);
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
    /// Get the Hive Metastore configuration of the most recently connected-to
    /// leader master, or null if the Hive Metastore integration is not enabled.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async ValueTask<HiveMetastoreConfig> GetHiveMetastoreConfigAsync(
        CancellationToken cancellationToken = default)
    {
        var masterLeaderInfo = await GetMasterLeaderInfoAsync(cancellationToken)
            .ConfigureAwait(false);

        return masterLeaderInfo.HiveMetastoreConfig;
    }

    /// <summary>
    /// Returns a string representation of this client's location. If this
    /// client was not assigned a location, returns the empty string.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async ValueTask<string> GetLocationAsync(
        CancellationToken cancellationToken = default)
    {
        var masterLeaderInfo = await GetMasterLeaderInfoAsync(cancellationToken)
            .ConfigureAwait(false);

        return masterLeaderInfo.Location;
    }

    /// <summary>
    /// Returns the ID of the cluster that this client is connected to.
    /// It will be an empty string if the client is connected to a cluster
    /// that doesn't support cluster IDs.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async ValueTask<string> GetClusterIdAsync(
        CancellationToken cancellationToken = default)
    {
        var masterLeaderInfo = await GetMasterLeaderInfoAsync(cancellationToken)
            .ConfigureAwait(false);

        return masterLeaderInfo.ClusterId;
    }

    /// <summary>
    /// Export serialized authentication data that may be passed to a different
    /// client instance and imported to provide that client the ability to connect
    /// to the cluster.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async ValueTask<ReadOnlyMemory<byte>> ExportAuthenticationCredentialsAsync(
        CancellationToken cancellationToken = default)
    {
        await GetMasterLeaderInfoAsync(cancellationToken).ConfigureAwait(false);
        return _securityContext.ExportAuthenticationCredentials();
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

    /// <summary>
    /// Create a table on the cluster with the specified name, schema, and
    /// table configurations.
    /// </summary>
    /// <param name="builder">The create table options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task<KuduTable> CreateTableAsync(
        TableBuilder builder, CancellationToken cancellationToken = default)
    {
        var rpc = new CreateTableRequest(builder.Build());

        var response = await SendRpcAsync(rpc, cancellationToken)
            .ConfigureAwait(false);

        var tableId = new TableIdentifierPB { TableId = response.TableId };

        if (builder.Wait)
        {
            await WaitForCreateTableDoneAsync(
                tableId, cancellationToken).ConfigureAwait(false);

            return await OpenTableAsync(
                tableId, cancellationToken).ConfigureAwait(false);
        }

        return null;
    }

    /// <summary>
    /// Alter a table on the cluster as specified by the builder.
    /// </summary>
    /// <param name="builder">The alter table options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task<AlterTableResponse> AlterTableAsync(
        AlterTableBuilder builder,
        CancellationToken cancellationToken = default)
    {
        var rpc = new AlterTableRequest(builder);
        AlterTableResponse response;

        try
        {
            response = await SendRpcAsync(rpc, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            if (builder.HasAddDropRangePartitions)
            {
                // Clear the table locations cache so the new partition is
                // immediately visible. We clear the cache even on failure,
                // just in case the alter table operation actually succeeded.
                _tableLocations.TryRemove(builder.TableId, out _);
            }
        }

        if (builder.Wait)
        {
            var isDoneResponse = await WaitForAlterTableDoneAsync(
                builder.TableIdPb, cancellationToken).ConfigureAwait(false);

            response = new AlterTableResponse(
                response.TableId,
                isDoneResponse.SchemaVersion);
        }

        return response;
    }

    private async Task<IsCreateTableDoneResponsePB> WaitForCreateTableDoneAsync(
        TableIdentifierPB tableId, CancellationToken cancellationToken)
    {
        var request = new IsCreateTableDoneRequestPB { Table = tableId };
        var rpc = new IsCreateTableDoneRequest(request);

        while (true)
        {
            var response = await SendRpcAsync(rpc, cancellationToken)
                .ConfigureAwait(false);

            if (response.Done)
                return response;

            rpc.Attempt++;
            await DelayRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<IsAlterTableDoneResponsePB> WaitForAlterTableDoneAsync(
        TableIdentifierPB tableId, CancellationToken cancellationToken)
    {
        var request = new IsAlterTableDoneRequestPB { Table = tableId };
        var rpc = new IsAlterTableDoneRequest(request);

        while (true)
        {
            var response = await SendRpcAsync(rpc, cancellationToken)
                .ConfigureAwait(false);

            if (response.Done)
                return response;

            rpc.Attempt++;
            await DelayRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Get a list of table names. Passing a null filter returns all the tables.
    /// When a filter is specified, it only returns tables that satisfy a substring
    /// match.
    /// </summary>
    /// <param name="nameFilter">An optional table name filter.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task<List<TableInfo>> GetTablesAsync(
        string nameFilter = null, CancellationToken cancellationToken = default)
    {
        var rpc = new ListTablesRequest(nameFilter);
        var response = await SendRpcAsync(rpc, cancellationToken)
            .ConfigureAwait(false);

        var tables = response.Tables;
        var results = new List<TableInfo>(tables.Count);

        foreach (var tablePb in tables)
        {
            var tableId = tablePb.Id.ToStringUtf8();
            var table = new TableInfo(tablePb.Name, tableId);
            results.Add(table);
        }

        return results;
    }

    /// <summary>
    /// Get the list of running tablet servers.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task<List<TabletServerInfo>> GetTabletServersAsync(
        CancellationToken cancellationToken = default)
    {
        var rpc = new ListTabletServersRequest();
        var response = await SendRpcAsync(rpc, cancellationToken)
            .ConfigureAwait(false);

        var servers = response.Servers;
        var results = new List<TabletServerInfo>(servers.Count);
        foreach (var serverPb in servers)
        {
            var serverInfo = serverPb.ToTabletServerInfo();
            results.Add(serverInfo);
        }

        return results;
    }

    /// <summary>
    /// Get a table's statistics from the master.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task<KuduTableStatistics> GetTableStatisticsAsync(
        string tableName, CancellationToken cancellationToken = default)
    {
        var table = new TableIdentifierPB { TableName = tableName };
        var request = new GetTableStatisticsRequestPB { Table = table };
        var rpc = new GetTableStatisticsRequest(request);
        var response = await SendRpcAsync(rpc, cancellationToken)
            .ConfigureAwait(false);

        return new KuduTableStatistics(response.OnDiskSize, response.LiveRowCount);
    }

    internal Task<List<RemoteTablet>> GetTableLocationsAsync(
        string tableId, byte[] partitionKeyStart, int fetchBatchSize,
        CancellationToken cancellationToken = default)
    {
        return GetTableLocationsAsync(
            tableId, partitionKeyStart, null, fetchBatchSize, cancellationToken);
    }

    internal async Task<List<RemoteTablet>> GetTableLocationsAsync(
        string tableId, byte[] partitionKeyStart, byte[] partitionKeyEnd,
        int fetchBatchSize, CancellationToken cancellationToken = default)
    {
        var request = new GetTableLocationsRequestPB
        {
            Table = new TableIdentifierPB { TableId = ByteString.CopyFromUtf8(tableId) },
            MaxReturnedLocations = (uint)fetchBatchSize,
            InternTsInfosInResponse = true
        };

        if (partitionKeyStart is not null)
        {
            request.PartitionKeyStart = UnsafeByteOperations.UnsafeWrap(partitionKeyStart);
        }

        if (partitionKeyEnd is not null)
        {
            request.PartitionKeyEnd = UnsafeByteOperations.UnsafeWrap(partitionKeyEnd);
        }

        var rpc = new GetTableLocationsRequest(request);
        var result = await SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);

        var tableLocations = await _connectionFactory.GetTabletsAsync(tableId, result)
            .ConfigureAwait(false);

        CacheTablets(
            tableId,
            tableLocations,
            partitionKeyStart,
            fetchBatchSize,
            result.TtlMillis);

        return tableLocations;
    }

    /// <summary>
    /// Open the table with the given name.
    /// </summary>
    /// <param name="tableName">The table to open.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public Task<KuduTable> OpenTableAsync(
        string tableName, CancellationToken cancellationToken = default)
    {
        var tableIdentifier = new TableIdentifierPB { TableName = tableName };
        return OpenTableAsync(tableIdentifier, cancellationToken);
    }

    /// <summary>
    /// Writes the given rows to Kudu without batching. For writing a large
    /// number of rows (>2000), consider using a session to handle batching.
    /// </summary>
    /// <param name="operations">The rows to write.</param>
    /// <param name="externalConsistencyMode">The external consistency mode for this write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public Task<WriteResponse> WriteAsync(
        IEnumerable<KuduOperation> operations,
        ExternalConsistencyMode externalConsistencyMode = ExternalConsistencyMode.ClientPropagated,
        CancellationToken cancellationToken = default)
    {
        return WriteAsync(operations, externalConsistencyMode, InvalidTxnId, cancellationToken);
    }

    internal async Task<WriteResponse> WriteAsync(
        IEnumerable<KuduOperation> operations,
        ExternalConsistencyMode externalConsistencyMode,
        long txnId,
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
                tabletOperations.Key,
                tabletOperations.Value,
                externalConsistencyMode,
                txnId,
                cancellationToken);

            tasks[i++] = task;
        }

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);

        return CreateWriteResponse(results);
    }

    private static WriteResponse CreateWriteResponse(WriteResponsePB[] results)
    {
        long writeTimestamp = NoTimestamp;
        List<KuduStatus> rowErrors = null;

        foreach (var result in results)
        {
            var timestamp = (long)result.Timestamp;
            var perRowErrors = result.PerRowErrors;

            if (writeTimestamp == NoTimestamp || writeTimestamp < timestamp)
            {
                writeTimestamp = timestamp;
            }

            if (perRowErrors.Count > 0)
            {
                rowErrors ??= new List<KuduStatus>();

                foreach (var rowError in perRowErrors)
                {
                    var status = KuduStatus.FromPB(rowError.Error);
                    rowErrors.Add(status);
                }
            }
        }

        if (rowErrors != null)
        {
            throw new KuduWriteException(rowErrors);
        }

        return new WriteResponse(writeTimestamp);
    }

    private async Task<WriteResponsePB> WriteAsync(
        RemoteTablet tablet,
        List<KuduOperation> operations,
        ExternalConsistencyMode externalConsistencyMode,
        long txnId,
        CancellationToken cancellationToken)
    {
        var table = operations[0].Table;

        OperationsEncoder.ComputeSize(
            operations,
            out int rowSize,
            out int indirectSize);

        using var rowData = MemoryPool<byte>.Shared.Rent(rowSize);
        using var indirectData = MemoryPool<byte>.Shared.Rent(indirectSize);
        var rowDataMemory = rowData.Memory.Slice(0, rowSize);
        var indirectDataMemory = indirectData.Memory.Slice(0, indirectSize);

        OperationsEncoder.Encode(operations, rowDataMemory.Span, indirectDataMemory.Span);

        var rowOperations = new RowOperationsPB
        {
            Rows = UnsafeByteOperations.UnsafeWrap(rowDataMemory),
            IndirectData = UnsafeByteOperations.UnsafeWrap(indirectDataMemory)
        };

        var request = new WriteRequestPB
        {
            Schema = table.SchemaPbNoIds.Schema,
            RowOperations = rowOperations,
            ExternalConsistencyMode = (ExternalConsistencyModePB)externalConsistencyMode
        };

        if (txnId != InvalidTxnId)
            request.TxnId = txnId;

        var rpc = new WriteRequest(
            request,
            table.TableId,
            tablet.Partition.PartitionKeyStart,
            externalConsistencyMode);

        var response = await SendRpcAsync(rpc, cancellationToken)
            .ConfigureAwait(false);

        LastPropagatedTimestamp = (long)response.Timestamp;

        return response;
    }

    /// <summary>
    /// Creates a new <see cref="KuduScannerBuilder"/> for a particular table.
    /// </summary>
    /// <param name="table">The table to scan.</param>
    public KuduScannerBuilder NewScanBuilder(KuduTable table)
    {
        return new KuduScannerBuilder(this, table, _scanLogger);
    }

    /// <summary>
    /// Creates a new <see cref="KuduScannerBuilder"/> from the given scan token.
    /// </summary>
    /// <param name="scanToken">The scan token to use.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public ValueTask<KuduScannerBuilder> NewScanBuilderFromTokenAsync(
        ReadOnlyMemory<byte> scanToken, CancellationToken cancellationToken = default)
    {
        var scanTokenPb = KuduScanToken.DeserializePb(scanToken.Span);
        return NewScanBuilderFromTokenAsync(scanTokenPb, cancellationToken);
    }

    /// <summary>
    /// Creates a new <see cref="KuduScannerBuilder"/> from the given scan token.
    /// </summary>
    /// <param name="scanToken">The scan token to use.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public ValueTask<KuduScannerBuilder> NewScanBuilderFromTokenAsync(
        KuduScanToken scanToken, CancellationToken cancellationToken = default)
    {
        return NewScanBuilderFromTokenAsync(scanToken.Message, cancellationToken);
    }

    private async ValueTask<KuduScannerBuilder> NewScanBuilderFromTokenAsync(
        ScanTokenPB scanTokenPb, CancellationToken cancellationToken)
    {
        var table = await OpenTableAsync(scanTokenPb, cancellationToken).ConfigureAwait(false);
        var scanBuilder = NewScanBuilder(table);
        KuduScanToken.PbIntoScanner(scanBuilder, scanTokenPb);

        // Prime the client tablet location cache if no entry is already present.
        var tableId = table.TableId;
        var tabletMetadata = scanTokenPb.TabletMetadata;
        if (tabletMetadata is not null &&
            GetTableLocationEntry(
                tableId,
                tabletMetadata.Partition.PartitionKeyStart.Memory.Span) is null)
        {
            var internedServers = new List<ServerInfo>(tabletMetadata.TabletServers.Count);

            foreach (var serverPb in tabletMetadata.TabletServers)
            {
                var hostPort = serverPb.RpcAddresses[0].ToHostAndPort();
                var serverInfo = await _connectionFactory.GetTabletServerInfoAsync(
                    hostPort,
                    serverPb.Uuid.ToStringUtf8(),
                    serverPb.Location,
                    cancellationToken).ConfigureAwait(false);

                internedServers.Add(serverInfo);
            }

            var servers = new List<ServerInfo>(tabletMetadata.Replicas.Count);
            var replicas = new List<KuduReplica>(tabletMetadata.Replicas.Count);
            int leaderIndex = -1;

            foreach (var replicaPb in tabletMetadata.Replicas)
            {
                var serverInfo = internedServers[(int)replicaPb.TsIdx];
                var role = (ReplicaRole)replicaPb.Role;
                var replica = new KuduReplica(
                    serverInfo.HostPort, role, replicaPb.DimensionLabel);

                if (role == ReplicaRole.Leader)
                {
                    leaderIndex = replicas.Count;
                }

                servers.Add(serverInfo);
                replicas.Add(replica);
            }

            var tableLocationsCache = GetTableLocationsCache(tableId);
            var partition = ProtobufHelper.ToPartition(tabletMetadata.Partition);
            var cache = new ServerInfoCache(servers, replicas, leaderIndex);
            var tablet = new RemoteTablet(tableId, tabletMetadata.TabletId, partition, cache);

            tableLocationsCache.CacheTabletLocations(
                new List<RemoteTablet> { tablet },
                partition.PartitionKeyStart,
                1,
                (long)tabletMetadata.TtlMillis);
        }

        if (scanTokenPb.AuthzToken is not null)
        {
            _authzTokenCache.SetAuthzToken(table.TableId, scanTokenPb.AuthzToken);
        }

        return scanBuilder;
    }

    public KuduScanTokenBuilder NewScanTokenBuilder(KuduTable table)
    {
        return new KuduScanTokenBuilder(this, table, _systemClock);
    }

    public IKuduSession NewSession() => NewSession(DefaultSessionOptions);

    public IKuduSession NewSession(KuduSessionOptions options)
    {
        return new KuduSession(this, options, _loggerFactory, InvalidTxnId);
    }

    /// <summary>
    /// Start a new multi-row distributed transaction.
    /// </summary>
    /// <param name="cancellationToken">Used to cancel creating the transaction.</param>
    public async Task<KuduTransaction> NewTransactionAsync(
        CancellationToken cancellationToken = default)
    {
        var rpc = new BeginTransactionRequest();
        var response = await SendRpcAsync(rpc, cancellationToken);

        var period = response.KeepaliveMillis / 2;
        var keepaliveMillis = period <= 0 ? 1 : period;
        var keepaliveInterval = TimeSpan.FromMilliseconds(keepaliveMillis);

        return new KuduTransaction(this, _loggerFactory, response.TxnId, keepaliveInterval);
    }

    /// <summary>
    /// <para>
    /// Re-create KuduTransaction object given its serialized representation.
    /// </para>
    /// 
    /// <para>
    /// The newly created object automatically does or does not send keep-alive messages
    /// depending on the <see cref="KuduTransactionSerializationOptions.EnableKeepalive"/>
    /// setting when the original <see cref="KuduTransaction"/> object was serialized.
    /// </para>
    /// </summary>
    /// <param name="transactionToken">Serialized representation of a KuduTransaction.</param>
    public KuduTransaction NewTransactionFromToken(ReadOnlyMemory<byte> transactionToken)
    {
        var tokenPb = TxnTokenPB.Parser.ParseFrom(transactionToken.Span);
        var txnId = tokenPb.TxnId;
        var keepaliveEnabled = tokenPb.HasEnableKeepalive && tokenPb.EnableKeepalive;
        var keepaliveInterval = keepaliveEnabled
            ? TimeSpan.FromMilliseconds(tokenPb.KeepaliveMillis)
            : TimeSpan.Zero;

        return new KuduTransaction(this, _loggerFactory, txnId, keepaliveInterval);
    }

    /// <summary>
    /// <para>
    /// Create a <see cref="KuduPartitioner"/> that allows clients to determine
    /// the target partition of a row without actually performing a write. The
    /// set of partitions is eagerly fetched when the KuduPartitioner is constructed
    /// so that the actual partitioning step can be performed synchronously
    /// without any network trips.
    /// </para>
    /// 
    /// <para>
    /// Note: Because this operates on a metadata snapshot retrieved at
    /// construction time, it will not reflect any metadata changes to the
    /// table that have occurred since its creation.
    /// </para>
    /// </summary>
    /// <param name="table">The table to operate on.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async ValueTask<KuduPartitioner> CreatePartitionerAsync(
        KuduTable table, CancellationToken cancellationToken = default)
    {
        var tablets = await LoopLocateTableAsync(
            table.TableId,
            null,
            null,
            1000,
            cancellationToken).ConfigureAwait(false);

        return new KuduPartitioner(table, tablets);
    }

    private async Task<KuduTable> OpenTableAsync(
        TableIdentifierPB tableIdentifier, CancellationToken cancellationToken)
    {
        var response = await GetTableSchemaAsync(tableIdentifier, cancellationToken)
            .ConfigureAwait(false);

        return new KuduTable(response);
    }

    private ValueTask<KuduTable> OpenTableAsync(
        ScanTokenPB scanTokenPb, CancellationToken cancellationToken)
    {
        // Use the table metadata from the scan token if it exists,
        // otherwise call OpenTable to get the metadata from the master.
        var tableMetadata = scanTokenPb.TableMetadata;

        if (tableMetadata is not null)
        {
            // TODO: This isn't the right type to pass to KuduTable.
            var schemaPb = new GetTableSchemaResponsePB
            {
                TableId = ByteString.CopyFromUtf8(tableMetadata.TableId),
                TableName = tableMetadata.TableName,
                NumReplicas = tableMetadata.NumReplicas,
                Schema = tableMetadata.Schema,
                PartitionSchema = tableMetadata.PartitionSchema
            };

            if (tableMetadata.HasOwner)
            {
                schemaPb.Owner = tableMetadata.Owner;
            }

            if (tableMetadata.HasComment)
            {
                schemaPb.Comment = tableMetadata.Comment;
            }

            if (tableMetadata.ExtraConfigs.Count > 0)
            {
                schemaPb.ExtraConfigs.Add(tableMetadata.ExtraConfigs);
            }

            var table = new KuduTable(schemaPb);
            return new ValueTask<KuduTable>(table);
        }
        else if (scanTokenPb.HasTableId)
        {
            var tableIdentifier = new TableIdentifierPB
            {
                TableId = ByteString.CopyFromUtf8(scanTokenPb.TableId)
            };

            var task = OpenTableAsync(tableIdentifier, cancellationToken);
            return new ValueTask<KuduTable>(task);
        }
        else
        {
            var task = OpenTableAsync(scanTokenPb.TableName, cancellationToken);
            return new ValueTask<KuduTable>(task);
        }
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
        if (authzToken is not null)
        {
            _authzTokenCache.SetAuthzToken(
                schema.TableId.ToStringUtf8(), authzToken);
        }
        else if (requiresAuthzTokenSupport)
        {
            throw new NonRecoverableException(KuduStatus.InvalidArgument(
                $"No authz token retrieved for {schema.TableName}"));
        }

        return schema;
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
        var entry = GetTableLocationEntry(tableId, partitionKey);

        if (entry is null)
            return null;

        if (entry.IsNonCoveredRange)
        {
            throw new NonCoveredRangeException(
                entry.LowerBoundPartitionKey,
                entry.UpperBoundPartitionKey);
        }

        return entry.Tablet;
    }

    /// <summary>
    /// Gets the tablet location cache entry for the tablet
    /// in the table covering a partition key. Returns null
    /// if the partition key has not been discovered.
    /// </summary>
    /// <param name="tableId">The table.</param>
    /// <param name="partitionKey">The partition key of the tablet to find.</param>
    internal TableLocationEntry GetTableLocationEntry(
        string tableId, ReadOnlySpan<byte> partitionKey)
    {
        var cache = GetTableLocationsCache(tableId);
        return cache.GetEntry(partitionKey);
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

        var result = tablets.FindTablet(partitionKey);

        if (result.IsNonCoveredRange)
        {
            throw new NonCoveredRangeException(
                result.NonCoveredRangeStart, result.NonCoveredRangeEnd);
        }

        return result.Tablet;
    }

    /// <summary>
    /// Adds the given tablets to the table location cache.
    /// </summary>
    /// <param name="tableId">The table identifier.</param>
    /// <param name="tablets">The discovered tablets to cache.</param>
    /// <param name="requestPartitionKey">The lookup partition key.</param>
    /// <param name="requestedBatchSize">
    /// The number of tablet locations requested from the master in the
    /// original request.
    /// </param>
    /// <param name="ttl">
    /// The time in milliseconds that the tablets may be cached for.
    /// </param>
    private void CacheTablets(
        string tableId,
        List<RemoteTablet> tablets,
        ReadOnlySpan<byte> requestPartitionKey,
        int requestedBatchSize,
        long ttl)
    {
        TableLocationsCache cache = GetTableLocationsCache(tableId);

        cache.CacheTabletLocations(
            tablets, requestPartitionKey, requestedBatchSize, ttl);
    }

    private void RemoveTabletFromCache<T>(KuduTabletRpc<T> rpc)
    {
        RemoteTablet tablet = rpc.Tablet;
        rpc.Tablet = null;

        TableLocationsCache cache = GetTableLocationsCache(tablet.TableId);
        cache.RemoveTablet(tablet.Partition.PartitionKeyStart);
    }

    private void RemoveTabletServerFromCache<T>(KuduTabletRpc<T> rpc, ServerInfo serverInfo)
    {
        RemoteTablet tablet = rpc.Tablet.RemoveTabletServer(serverInfo.Uuid);
        rpc.Tablet = tablet;

        TableLocationsCache cache = GetTableLocationsCache(tablet.TableId);
        cache.UpdateTablet(tablet);
    }

    private TableLocationsCache GetTableLocationsCache(string tableId)
    {
        return _tableLocations.GetOrAdd(
            tableId,
            static (key, clock) => new TableLocationsCache(clock),
            _systemClock);
    }

    private async ValueTask<List<RemoteTablet>> LoopLocateTableAsync(
        string tableId,
        byte[] startPartitionKey,
        byte[] endPartitionKey,
        int fetchBatchSize,
        CancellationToken cancellationToken = default)
    {
        var tablets = new List<RemoteTablet>();

        await LoopLocateTableAsync(
            tableId,
            startPartitionKey,
            endPartitionKey,
            fetchBatchSize,
            tablets,
            cancellationToken).ConfigureAwait(false);

        return tablets;
    }

    private async ValueTask LoopLocateTableAsync(
        string tableId,
        byte[] startPartitionKey,
        byte[] endPartitionKey,
        int fetchBatchSize,
        List<RemoteTablet> results,
        CancellationToken cancellationToken = default)
    {
        // We rely on the keys initially not being empty.
        if (startPartitionKey != null && startPartitionKey.Length == 0)
            throw new ArgumentException("Use null for unbounded start partition key");

        if (endPartitionKey != null && endPartitionKey.Length == 0)
            throw new ArgumentException("Use null for unbounded end partition key");

        // The next partition key to look up. If null, then it represents
        // the minimum partition key, If empty, it represents the maximum key.
        byte[] partitionKey = startPartitionKey;

        // Continue while the partition key is the minimum, or it is not the maximum
        // and it is less than the end partition key.
        while (partitionKey == null ||
               (partitionKey.Length > 0 &&
                (endPartitionKey == null || partitionKey.SequenceCompareTo(endPartitionKey) < 0)))
        {
            byte[] key = partitionKey ?? Array.Empty<byte>();
            var entry = GetTableLocationEntry(tableId, key);

            if (entry != null)
            {
                if (entry.IsCoveredRange)
                    results.Add(entry.Tablet);

                partitionKey = entry.UpperBoundPartitionKey;
                continue;
            }

            // If the partition key location isn't cached, and the request hasn't timed out,
            // then kick off a new tablet location lookup and try again when it completes.
            // When lookup completes, the tablet (or non-covered range) for the next
            // partition key will be located and added to the client's cache.
            byte[] lookupKey = partitionKey;

            await GetTableLocationsAsync(tableId, key, fetchBatchSize, cancellationToken)
                .ConfigureAwait(false);

            await LoopLocateTableAsync(
                tableId,
                lookupKey,
                endPartitionKey,
                fetchBatchSize,
                results,
                cancellationToken).ConfigureAwait(false);

            break;
        }
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
    private Task<SplitKeyRangeResponsePB> GetTabletKeyRangesAsync(
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

    internal async ValueTask<List<KeyRange>> GetTableKeyRangesAsync(
        string tableId,
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        byte[] startPartitionKey,
        byte[] endPartitionKey,
        int fetchBatchSize,
        long splitSizeBytes,
        CancellationToken cancellationToken = default)
    {
        var tablets = await LoopLocateTableAsync(
            tableId,
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
                    keyRangePb.StartPrimaryKey.Memory,
                    keyRangePb.StopPrimaryKey.Memory,
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
            var response = await client.GetTabletKeyRangesAsync(
                tablet.TableId,
                startPrimaryKey,
                endPrimaryKey,
                tablet.Partition.PartitionKeyStart,
                splitSizeBytes,
                cancellationToken).ConfigureAwait(false);

            return new SplitKeyRangeResponse(tablet, response.Ranges);
        }
    }

    private ValueTask<MasterLeaderInfo> GetMasterLeaderInfoAsync(
        CancellationToken cancellationToken)
    {
        var masterLeaderInfo = _masterLeaderInfo;
        if (masterLeaderInfo is not null)
        {
            return new ValueTask<MasterLeaderInfo>(masterLeaderInfo);
        }

        var task = ConnectToClusterAsync(cancellationToken);
        return new ValueTask<MasterLeaderInfo>(task);
    }

    /// <summary>
    /// Locate the leader master and retrieve the cluster information.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task<MasterLeaderInfo> ConnectToClusterAsync(CancellationToken cancellationToken)
    {
        var masterLeaderInfo = _masterLeaderInfo;

        await _singleClusterConnect.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (ReferenceEquals(masterLeaderInfo, _masterLeaderInfo) ||
                _masterLeaderInfo is null)
            {
                var result = await ConnectToClusterWithoutLockAsync(cancellationToken)
                    .ConfigureAwait(false);

                SetMasterLeaderInfo(result);
            }
        }
        finally
        {
            _singleClusterConnect.Release();
        }

        return _masterLeaderInfo;
    }

    private async Task<ConnectToClusterResponse> ConnectToClusterWithoutLockAsync(
        CancellationToken cancellationToken)
    {
        var masterAddresses = _options.MasterAddresses;
        var tasks = new Dictionary<Task<ConnectToMasterResponse>, HostAndPort>(masterAddresses.Count);
        var builder = new ExceptionBuilder();

        using var cts = new CancellationTokenSource();
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cts.Token, cancellationToken);

        // Attempt to connect to all configured masters in parallel.
        foreach (var address in masterAddresses)
        {
            var task = ConnectToMasterAsync(address, builder, cancellationToken);
            tasks.Add(task, address);
        }

        ServerInfo leaderServerInfo = null;
        ConnectToMasterResponsePB leaderResponsePb = null;
        IReadOnlyList<HostPortPB> clusterMasterAddresses = null;

        while (tasks.Count > 0)
        {
            var task = await Task.WhenAny(tasks.Keys).ConfigureAwait(false);
            tasks.Remove(task, out var hostPort);

            ConnectToMasterResponse response;

            try
            {
                response = await task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                builder.AddException(hostPort, ex);
                continue;
            }

            clusterMasterAddresses = response.ClusterMasterAddresses;

            if (response.IsLeader)
            {
                leaderServerInfo = response.LeaderServerInfo;
                leaderResponsePb = response.LeaderResponsePb;

                // Found the leader, that's all we care about.
                cts.Cancel();
                break;
            }
        }

        if (clusterMasterAddresses?.Count > 0 &&
            clusterMasterAddresses.Count != masterAddresses.Count)
        {
            _logger.MisconfiguredMasterAddresses(masterAddresses, clusterMasterAddresses);
        }

        if (leaderServerInfo is null)
        {
            var exception = builder.CreateException();
            throw exception;
        }

        return new ConnectToClusterResponse(leaderServerInfo, leaderResponsePb);
    }

    private async Task<ConnectToMasterResponse> ConnectToMasterAsync(
        HostAndPort hostPort, ExceptionBuilder builder, CancellationToken cancellationToken)
    {
        var servers = await _connectionFactory.GetMasterServerInfoAsync(
            hostPort, cancellationToken).ConfigureAwait(false);

        var tasks = new Dictionary<Task<ConnectToMasterResponsePB>, ServerInfo>(servers.Count);

        foreach (var serverInfo in servers)
        {
            var task = ConnectToMasterAsync(serverInfo, cancellationToken);
            tasks.Add(task, serverInfo);
        }

        ServerInfo leaderServerInfo = null;
        ConnectToMasterResponsePB leaderResponsePb = null;
        IReadOnlyList<HostPortPB> clusterMasterAddresses = null;

        while (tasks.Count > 0)
        {
            var task = await Task.WhenAny(tasks.Keys).ConfigureAwait(false);
            tasks.Remove(task, out var serverInfo);

            ConnectToMasterResponsePB responsePb;

            try
            {
                responsePb = await task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Failed to connect to this master.
                // Failures are fine here, as long as we can
                // connect to the leader.

                builder.AddException(serverInfo, ex);
                continue;
            }

            clusterMasterAddresses = responsePb.MasterAddrs;

            if (responsePb.Role == RaftPeerPB.Types.Role.Leader)
            {
                leaderServerInfo = serverInfo;
                leaderResponsePb = responsePb;

                // Found the leader, that's all we care about.
                break;
            }

            builder.AddNonLeaderMaster(serverInfo);
        }

        return new ConnectToMasterResponse(
            leaderResponsePb,
            leaderServerInfo,
            clusterMasterAddresses);
    }

    private Task<ConnectToMasterResponsePB> ConnectToMasterAsync(
        ServerInfo serverInfo, CancellationToken cancellationToken)
    {
        var rpc = new ConnectToMasterRequest();
        return SendRpcToMasterAsync(rpc, serverInfo, cancellationToken);
    }

    private void SetMasterLeaderInfo(ConnectToClusterResponse clusterResponse)
    {
        var responsePb = clusterResponse.ResponsePb;
        var authnToken = responsePb.AuthnToken;
        if (authnToken is not null)
        {
            // If the response has security info, adopt it.
            _securityContext.SetAuthenticationToken(authnToken);
        }

        var certificates = responsePb.CaCertDer;
        if (certificates.Count > 0)
        {
            // TODO: Log any exceptions from this.
            _securityContext.TrustCertificates(certificates.ToMemoryArray());
        }

        var masterLeaderInfo = new MasterLeaderInfo(
            responsePb.ClientLocation,
            responsePb.ClusterId,
            clusterResponse.ServerInfo,
            responsePb.HmsConfig.ToHiveMetastoreConfig());

        _masterLeaderInfo = masterLeaderInfo;
    }

    internal async Task<T> SendRpcAsync<T>(
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
                    var task = rpc switch
                    {
                        KuduMasterRpc<T> masterRpc => SendRpcToMasterAsync(masterRpc, token),
                        KuduTabletRpc<T> tabletRpc => SendRpcToTabletAsync(tabletRpc, token),
                        KuduTxnRpc<T> txnRpc => SendRpcToTxnAsync(txnRpc, token),
                        _ => throw new NotSupportedException()
                    };

                    return await task.ConfigureAwait(false);
                }
                catch (RecoverableException ex)
                {
                    // Record the exception and retry.
                    HandleRpcException(rpc, ex, token);
                    await DelayRpcAsync(rpc, token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw new OperationCanceledException(
                $"Couldn't complete RPC before timeout ({rpc.Attempt} attempts)",
                rpc.Exception);
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

    private async Task<T> SendRpcToMasterAsync<T>(
        KuduMasterRpc<T> rpc, CancellationToken cancellationToken)
    {
        var masterLeaderInfo = await GetMasterLeaderInfoAsync(cancellationToken)
            .ConfigureAwait(false);

        var serverInfo = masterLeaderInfo.ServerInfo;

        return await SendRpcToMasterAsync(rpc, serverInfo, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<T> SendRpcToTxnAsync<T>(
        KuduTxnRpc<T> rpc, CancellationToken cancellationToken)
    {
        var masterLeaderInfo = await GetMasterLeaderInfoAsync(cancellationToken)
            .ConfigureAwait(false);

        var serverInfo = masterLeaderInfo.ServerInfo;

        return await SendRpcToTxnAsync(rpc, serverInfo, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Sends the provided <see cref="KuduTabletRpc{T}"/> to the server
    /// identified by RPC's table, partition key, and replica selection.
    /// </summary>
    /// <param name="rpc">The RPC to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task<T> SendRpcToTabletAsync<T>(
        KuduTabletRpc<T> rpc, CancellationToken cancellationToken)
    {
        // Set the propagated timestamp so that the next time we send a message to
        // the server the message includes the last propagated timestamp.
        if (rpc.ExternalConsistencyMode == ExternalConsistencyMode.ClientPropagated)
        {
            long lastPropagatedTimestamp = LastPropagatedTimestamp;

            if (lastPropagatedTimestamp != NoTimestamp)
                rpc.PropagatedTimestamp = lastPropagatedTimestamp;
        }

        string tableId = rpc.TableId;

        // Before we create the request, get an authz token if needed. This is done
        // regardless of whether the KuduRpc object already has a token; we may be
        // a retrying due to an invalid token and the client may have a new token.
        if (rpc.NeedsAuthzToken)
        {
            rpc.AuthzToken = _authzTokenCache.GetAuthzToken(tableId);
        }

        RemoteTablet tablet = rpc.Tablet;

        if (tablet == null)
        {
            tablet = await GetTabletAsync(
                tableId,
                rpc.PartitionKey,
                cancellationToken).ConfigureAwait(false);

            rpc.Tablet = tablet;
        }

        ServerInfo serverInfo = GetServerInfo(tablet, rpc.ReplicaSelection);

        if (serverInfo == null)
        {
            RemoveTabletFromCache(rpc);

            throw new RecoverableException(KuduStatus.IllegalState(
                $"Unable to find {rpc.ReplicaSelection} replica in {tablet}"));
        }

        return await SendRpcToTabletAsync(rpc, serverInfo, cancellationToken)
            .ConfigureAwait(false);
    }

    private ServerInfo GetServerInfo(RemoteTablet tablet, ReplicaSelection replicaSelection)
    {
        var masterLeaderInfo = _masterLeaderInfo;
        return tablet.GetServerInfo(replicaSelection, masterLeaderInfo?.Location);
    }

    internal SignedTokenPB GetAuthzToken(string tableId)
    {
        return _authzTokenCache.GetAuthzToken(tableId);
    }

    internal async ValueTask<HostAndPort> FindLeaderMasterServerAsync(
        CancellationToken cancellationToken = default)
    {
        var masterLeaderInfo = await GetMasterLeaderInfoAsync(cancellationToken)
            .ConfigureAwait(false);

        return masterLeaderInfo.ServerInfo.HostPort;
    }

    private async Task<T> SendRpcToMasterAsync<T>(
        KuduMasterRpc<T> rpc,
        ServerInfo serverInfo,
        CancellationToken cancellationToken)
    {
        await SendRpcToServerGenericAsync(rpc, serverInfo, cancellationToken)
            .ConfigureAwait(false);

        var error = rpc.Error;
        if (error is not null)
        {
            var errCode = error.Code;
            var errStatusCode = error.Status.Code;
            var status = KuduStatus.FromMasterErrorPB(error);

            if (errCode == MasterErrorPB.Types.Code.NotTheLeader)
            {
                InvalidateMasterServerCache();
                throw new RecoverableException(status);
            }
            else if (errStatusCode == AppStatusPB.Types.ErrorCode.ServiceUnavailable)
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

    private async Task<T> SendRpcToTxnAsync<T>(
        KuduTxnRpc<T> rpc,
        ServerInfo serverInfo,
        CancellationToken cancellationToken)
    {
        await SendRpcToServerGenericAsync(rpc, serverInfo, cancellationToken)
            .ConfigureAwait(false);

        var error = rpc.Error;
        if (error is not null)
        {
            var errStatusCode = error.Status.Code;
            var status = KuduStatus.FromTxnManagerErrorPB(error);

            if (errStatusCode == AppStatusPB.Types.ErrorCode.ServiceUnavailable)
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

    private async Task<T> SendRpcToTabletAsync<T>(
        KuduTabletRpc<T> rpc,
        ServerInfo serverInfo,
        CancellationToken cancellationToken)
    {
        await SendRpcToServerGenericAsync(rpc, serverInfo, cancellationToken)
            .ConfigureAwait(false);

        var error = rpc.Error;
        if (error is not null)
        {
            var errCode = error.Code;
            var errStatusCode = error.Status.Code;
            var status = KuduStatus.FromTabletServerErrorPB(error);

            if (errCode == TabletServerErrorPB.Types.Code.TabletNotFound ||
                errCode == TabletServerErrorPB.Types.Code.TabletNotRunning)
            {
                // We're handling a tablet server that's telling us it doesn't
                // have the tablet we're asking for.
                RemoveTabletServerFromCache(rpc, serverInfo);
                throw new RecoverableException(status);
            }
            else if (errStatusCode == AppStatusPB.Types.ErrorCode.ServiceUnavailable)
            {
                throw new RecoverableException(status);
            }
            else if (
                (errStatusCode == AppStatusPB.Types.ErrorCode.IllegalState &&
                errCode != TabletServerErrorPB.Types.Code.TxnIllegalState) ||
                errStatusCode == AppStatusPB.Types.ErrorCode.Aborted)
            {
                // These two error codes are an indication that the tablet
                // isn't a leader.
                RemoveTabletFromCache(rpc);
                throw new RecoverableException(status);
            }
            else
            {
                throw new NonRecoverableException(status);
            }
        }

        return rpc.Output;
    }

    private async Task SendRpcToServerGenericAsync<T>(
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
            if (connection is not null)
                await connection.CloseAsync().ConfigureAwait(false);

            throw;
        }
        catch (InvalidAuthzTokenException) when (rpc is KuduTabletRpc<T> tabletRpc)
        {
            await RefreshAuthzTokenAsync(tabletRpc.TableId, cancellationToken)
                .ConfigureAwait(false);

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
                    RemoveTabletFromCache(tabletRpc);
                }
                else
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

    private async Task RefreshAuthzTokenAsync(
        string tableId, CancellationToken cancellationToken)
    {
        var tableIdPb = new TableIdentifierPB
        {
            TableId = ByteString.CopyFromUtf8(tableId)
        };

        // This call will cache the authz token.
        await GetTableSchemaAsync(
            tableIdPb,
            requiresAuthzTokenSupport: true,
            cancellationToken).ConfigureAwait(false);
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
            TimeoutMillis = (uint)_defaultOperationTimeoutMs
        };

        var requiredFeatures = rpc.RequiredFeatures;
        if (requiredFeatures is not null)
        {
            header.RequiredFeatureFlags.AddRange(requiredFeatures);
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

    internal static Task DelayRpcAsync(KuduRpc rpc, CancellationToken cancellationToken)
    {
        int attemptCount = rpc.Attempt;

        // Randomized exponential backoff, truncated at 4096ms.
        int sleepTime = (int)(Math.Pow(2.0, Math.Min(attemptCount, 12)) *
            ThreadSafeRandom.Instance.NextDouble());

        return Task.Delay(sleepTime, cancellationToken);
    }

    private void HandleRpcException(KuduRpc rpc, Exception exception, CancellationToken cancellationToken)
    {
        // Record the last exception, so if the rpc times out we can report it.
        if (!cancellationToken.IsCancellationRequested || rpc.Exception is null)
        {
            rpc.Exception = exception;
        }

        rpc.Attempt++;

        _logger.RecoverableRpcException(exception);

        var numAttempts = rpc.Attempt;
        if (numAttempts > MaxRpcAttempts)
        {
            // TODO: OperationCanceledException?
            throw new NonRecoverableException(
                KuduStatus.TimedOut($"Too many RPC attempts: {numAttempts}"),
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

    private readonly struct SplitKeyRangeResponse
    {
        public readonly RemoteTablet Tablet;

        public readonly IReadOnlyList<KeyRangePB> KeyRanges;

        public SplitKeyRangeResponse(
            RemoteTablet tablet,
            IReadOnlyList<KeyRangePB> keyRanges)
        {
            Tablet = tablet;
            KeyRanges = keyRanges;
        }
    }

    private sealed record MasterLeaderInfo(
        string Location,
        string ClusterId,
        ServerInfo ServerInfo,
        HiveMetastoreConfig HiveMetastoreConfig);

    private sealed class ConnectToMasterResponse
    {
        public ConnectToMasterResponsePB LeaderResponsePb { get; }

        public ServerInfo LeaderServerInfo { get; }

        public IReadOnlyList<HostPortPB> ClusterMasterAddresses { get; }

        public ConnectToMasterResponse(
            ConnectToMasterResponsePB leaderResponsePb,
            ServerInfo leaderServerInfo,
            IReadOnlyList<HostPortPB> clusterMasterAddresses)
        {
            LeaderResponsePb = leaderResponsePb;
            LeaderServerInfo = leaderServerInfo;
            ClusterMasterAddresses = clusterMasterAddresses;
        }

        public bool IsLeader => LeaderServerInfo is not null;
    }

    private readonly struct ConnectToClusterResponse
    {
        public readonly ServerInfo ServerInfo;

        public readonly ConnectToMasterResponsePB ResponsePb;

        public ConnectToClusterResponse(
            ServerInfo serverInfo,
            ConnectToMasterResponsePB responsePb)
        {
            ServerInfo = serverInfo;
            ResponsePb = responsePb;
        }
    }

    private sealed class ExceptionBuilder
    {
        private readonly List<string> _results = new(3);
        private readonly List<Exception> _exceptions = new(0);

        public void AddNonLeaderMaster(ServerInfo serverInfo)
        {
            var result = $"{serverInfo.HostPort} [{serverInfo.Endpoint.Address}] => Not the leader";

            lock (_results)
            {
                _results.Add(result);
            }
        }

        public void AddException(ServerInfo serverInfo, Exception exception)
        {
            var result = $"{serverInfo.HostPort} [{serverInfo.Endpoint.Address}] => {exception.Message}";

            lock (_results)
            {
                _results.Add(result);
                _exceptions.Add(exception);
            }
        }

        public void AddException(HostAndPort hostPort, Exception exception)
        {
            var result = $"{hostPort} => {exception.Message}";

            lock (_results)
            {
                _results.Add(result);
                _exceptions.Add(exception);
            }
        }

        public NoLeaderFoundException CreateException()
        {
            List<string> results;
            List<Exception> exceptions;

            lock (_results)
            {
                results = new List<string>(_results);
                exceptions = new List<Exception>(_exceptions);
            }

            var innerException = new AggregateException(exceptions);
            return new NoLeaderFoundException(results, innerException);
        }
    }
}
