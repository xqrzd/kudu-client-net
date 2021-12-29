using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client;

public sealed class KuduScanEnumerator : IAsyncEnumerator<ResultSet>
{
    private readonly ILogger _logger;
    private readonly KuduClient _client;
    private readonly KuduTable _table;
    private readonly List<ColumnSchemaPB> _columns;
    private readonly KuduSchema _schema;
    private readonly PartitionPruner _partitionPruner;
    private readonly Dictionary<string, KuduPredicate> _predicates;
    private readonly CancellationToken _cancellationToken;

    private readonly OrderModePB _orderMode;
    private readonly ReadMode _readMode;
    private readonly ReplicaSelection _replicaSelection;
    private readonly bool _isFaultTolerant;
    private readonly int _batchSizeBytes;
    private readonly long _limit;
    private readonly bool _cacheBlocks;
    private readonly long _startTimestamp;
    private readonly long _lowerBoundPropagationTimestamp = KuduClient.NoTimestamp;

    private readonly byte[] _startPrimaryKey;
    private readonly byte[] _endPrimaryKey;

    private bool _closed;
    private long _numRowsReturned;
    private uint _sequenceId;
    private ByteString _scannerId;
    private ByteString _lastPrimaryKey;

    /// <summary>
    /// The tabletSlice currently being scanned.
    /// If null, we haven't started scanning.
    /// If == DONE, then we're done scanning.
    /// Otherwise it contains a proper tabletSlice name, and we're currently scanning.
    /// </summary>
    internal RemoteTablet? Tablet { get; private set; }

    internal long SnapshotTimestamp { get; private set; }

    public ResultSet Current { get; private set; } = null!;

    public ResourceMetrics ResourceMetrics { get; }

    public KuduScanEnumerator(
        ILogger logger,
        KuduClient client,
        KuduTable table,
        List<ColumnSchemaPB> projectedColumnsPb,
        KuduSchema projectionSchema,
        OrderModePB orderMode,
        ReadMode readMode,
        ReplicaSelection replicaSelection,
        bool isFaultTolerant,
        Dictionary<string, KuduPredicate> predicates,
        long limit,
        bool cacheBlocks,
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        long startTimestamp,
        long htTimestamp,
        int batchSizeBytes,
        PartitionPruner partitionPruner,
        CancellationToken cancellationToken)
    {
        _logger = logger;
        _client = client;
        _table = table;
        _partitionPruner = partitionPruner;
        _orderMode = orderMode;
        _readMode = readMode;
        _columns = projectedColumnsPb;
        _schema = projectionSchema;
        _predicates = predicates;
        _replicaSelection = replicaSelection;
        _isFaultTolerant = isFaultTolerant;
        _limit = limit;
        _cacheBlocks = cacheBlocks;
        _startPrimaryKey = startPrimaryKey ?? Array.Empty<byte>();
        _endPrimaryKey = endPrimaryKey ?? Array.Empty<byte>();
        _startTimestamp = startTimestamp;
        SnapshotTimestamp = htTimestamp;
        _batchSizeBytes = batchSizeBytes;
        _scannerId = ByteString.Empty;
        _lastPrimaryKey = ByteString.Empty;
        _cancellationToken = cancellationToken;
        ResourceMetrics = new ResourceMetrics();

        // If the partition pruner has pruned all partitions, then the scan can be
        // short circuited without contacting any tablet servers.
        if (!_partitionPruner.HasMorePartitionKeyRanges)
        {
            _closed = true;
        }

        // For READ_YOUR_WRITES scan mode, get the latest observed timestamp
        // and store it. Always use this one as the propagated timestamp for
        // the duration of the scan to avoid unnecessary wait.
        if (readMode == ReadMode.ReadYourWrites)
        {
            _lowerBoundPropagationTimestamp = client.LastPropagatedTimestamp;
        }
    }

    public async ValueTask DisposeAsync()
    {
        ClearCurrent();

        if (!_closed)
        {
            // Getting a null tablet here without being in a closed state
            // means we were in between tablets.
            if (Tablet != null)
            {
                try
                {
                    using var rpc = GetCloseRequest();
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                    await _client.SendRpcAsync(rpc, cts.Token)
                        .ConfigureAwait(false);
                }
                catch { }
            }

            _closed = true;
            Invalidate();
        }
    }

    public ValueTask<bool> MoveNextAsync()
    {
        ClearCurrent();

        if (_closed)
        {
            // We're already done scanning.
            return new ValueTask<bool>(false);
        }
        else if (Tablet == null)
        {
            // We need to open the scanner first.
            return OpenScannerAsync();
        }
        else
        {
            return ScanNextRowsAsync();
        }
    }

    /// <summary>
    /// <para>
    /// Keep the current remote scanner alive on the Tablet server for an
    /// additional time-to-live. This is useful if the interval in between
    /// <see cref="MoveNextAsync"/> calls is big enough that the remote
    /// scanner might be garbage collected. The scanner time-to-live can
    /// be configured on the tablet server via the --scanner_ttl_ms
    /// configuration flag and has a default of 60 seconds.
    /// </para>
    /// <para>
    /// This does not invalidate any previously fetched results.
    /// </para>
    /// <para>
    /// Note that an exception thrown by this method should not be taken as
    /// indication that the scan has failed. Subsequent calls to
    /// <see cref="MoveNextAsync"/> might still be successful, particularly
    /// if the scanner is configured to be fault tolerant.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public Task KeepAliveAsync(CancellationToken cancellationToken = default)
    {
        if (_closed)
        {
            throw new Exception("Scanner has already been closed");
        }

        if (Tablet is null)
        {
            // Getting a null tablet here without being in a closed state
            // means we were in between tablets. If there is no scanner to
            // keep alive, we still return success.

            return Task.CompletedTask;
        }

        var rpc = GetKeepAliveRequest();
        return _client.SendRpcAsync(rpc, cancellationToken);
    }

    private async ValueTask<bool> OpenScannerAsync()
    {
        using var rpc = GetOpenRequest();
        ScanResponsePB response;

        try
        {
            response = await _client.SendRpcAsync(rpc, _cancellationToken)
                .ConfigureAwait(false);
        }
        catch (NonCoveredRangeException ex)
        {
            Invalidate();
            _partitionPruner.RemovePartitionKeyRange(ex.NonCoveredRangeEnd);

            // Stop scanning if the non-covered range is past the end partition key.
            if (!_partitionPruner.HasMorePartitionKeyRanges)
            {
                // The scanner is closed on the other side at this point.
                _closed = true;
                return false;
            }

            _scannerId = ByteString.Empty;
            _sequenceId = 0;
            return await MoveNextAsync().ConfigureAwait(false);
        }
        catch
        {
            Invalidate();
            throw;
        }

        Tablet = rpc.Tablet;

        var scanTimestamp = response.HasSnapTimestamp
            ? (long)response.SnapTimestamp
            : KuduClient.NoTimestamp;

        var propagatedTimestamp = response.HasPropagatedTimestamp
            ? (long)response.PropagatedTimestamp
            : KuduClient.NoTimestamp;

        var lastPrimaryKey = response.HasLastPrimaryKey
            ? response.LastPrimaryKey.ToByteArray()
            : null;

        if (SnapshotTimestamp == KuduClient.NoTimestamp &&
            scanTimestamp != KuduClient.NoTimestamp)
        {
            // If the server-assigned timestamp is present in the tablet
            // server's response, store it in the scanner. The stored value
            // is used for read operations in READ_AT_SNAPSHOT mode at
            // other tablet servers in the context of the same scan.
            SnapshotTimestamp = scanTimestamp;
        }

        long lastPropagatedTimestamp = KuduClient.NoTimestamp;
        if (_readMode == ReadMode.ReadYourWrites &&
            scanTimestamp != KuduClient.NoTimestamp)
        {
            // For READ_YOUR_WRITES mode, update the latest propagated timestamp
            // with the chosen snapshot timestamp sent back from the server, to
            // avoid unnecessarily wait for subsequent reads. Since as long as
            // the chosen snapshot timestamp of the next read is greater than
            // the previous one, the scan does not violate READ_YOUR_WRITES
            // session guarantees.
            lastPropagatedTimestamp = scanTimestamp;
        }
        else if (propagatedTimestamp != KuduClient.NoTimestamp)
        {
            // Otherwise we just use the propagated timestamp returned from
            // the server as the latest propagated timestamp.
            lastPropagatedTimestamp = propagatedTimestamp;
        }
        if (lastPropagatedTimestamp != KuduClient.NoTimestamp)
        {
            _client.LastPropagatedTimestamp = lastPropagatedTimestamp;
        }

        if (_isFaultTolerant && response.HasLastPrimaryKey)
        {
            _lastPrimaryKey = response.LastPrimaryKey;
        }

        Current = rpc.TakeResultSet();

        var numRows = Current.Count;
        _numRowsReturned += numRows;

        if (response.ResourceMetrics is not null)
            ResourceMetrics.Update(response.ResourceMetrics);

        if (!response.HasMoreResults || !response.HasScannerId)
        {
            ScanFinished();

            if (numRows == 0 && _partitionPruner.HasMorePartitionKeyRanges)
            {
                return await MoveNextAsync().ConfigureAwait(false);
            }

            return numRows > 0;
        }

        _scannerId = response.ScannerId;
        _sequenceId++;

        return numRows > 0;
    }

    private async ValueTask<bool> ScanNextRowsAsync()
    {
        using var rpc = GetNextRowsRequest();
        ScanResponsePB response;

        try
        {
            response = await _client.SendRpcAsync(rpc, _cancellationToken)
                .ConfigureAwait(false);
        }
        catch (FaultTolerantScannerExpiredException)
        {
            _logger.ScannerExpired(_scannerId, _table.TableName, Tablet);

            // If encountered FaultTolerantScannerExpiredException, it means the
            // fault tolerant scanner on the server side expired. Therefore, open
            // a new scanner.
            Invalidate();

            _scannerId = ByteString.Empty;
            _sequenceId = 0;

            return await MoveNextAsync().ConfigureAwait(false);
        }
        catch
        {
            // If there was an error, don't assume we're still OK.
            Invalidate();
            throw;
        }

        Tablet = rpc.Tablet;
        Current = rpc.TakeResultSet();

        var numRows = Current.Count;
        _numRowsReturned += numRows;

        if (response.ResourceMetrics is not null)
            ResourceMetrics.Update(response.ResourceMetrics);

        if (!response.HasMoreResults)
        {
            // We're done scanning this tablet.
            ScanFinished();
            return numRows > 0;
        }
        _sequenceId++;

        return numRows > 0;
    }

    private ScanRequest GetOpenRequest()
    {
        var request = new ScanRequestPB();

        var newRequest = request.NewScanRequest = new NewScanRequestPB
        {
            Limit = (ulong)(_limit - _numRowsReturned),
            OrderMode = _orderMode,
            CacheBlocks = _cacheBlocks,
            ReadMode = (ReadModePB)_readMode,
            RowFormatFlags = (ulong)RowFormatFlags.ColumnarLayout
        };

        newRequest.ProjectedColumns.AddRange(_columns);

        // For READ_YOUR_WRITES scan, use the propagated timestamp from
        // the scanner.
        if (_readMode == ReadMode.ReadYourWrites)
        {
            long timestamp = _lowerBoundPropagationTimestamp;
            if (timestamp != KuduClient.NoTimestamp)
                newRequest.PropagatedTimestamp = (ulong)timestamp;
        }

        // If the mode is set to read on snapshot set the snapshot timestamps.
        if (_readMode == ReadMode.ReadAtSnapshot)
        {
            if (SnapshotTimestamp != KuduClient.NoTimestamp)
                newRequest.SnapTimestamp = (ulong)SnapshotTimestamp;

            if (_startTimestamp != KuduClient.NoTimestamp)
                newRequest.SnapStartTimestamp = (ulong)_startTimestamp;
        }

        if (_isFaultTolerant && _lastPrimaryKey.Length > 0)
            newRequest.LastPrimaryKey = _lastPrimaryKey;

        if (_startPrimaryKey.Length > 0)
            newRequest.StartPrimaryKey = UnsafeByteOperations.UnsafeWrap(_startPrimaryKey);

        if (_endPrimaryKey.Length > 0)
            newRequest.StopPrimaryKey = UnsafeByteOperations.UnsafeWrap(_endPrimaryKey);

        foreach (KuduPredicate predicate in _predicates.Values)
            newRequest.ColumnPredicates.Add(predicate.ToProtobuf());

        request.BatchSizeBytes = (uint)_batchSizeBytes;

        return new ScanRequest(
            ScanRequestState.Opening,
            request,
            _schema,
            _replicaSelection,
            _table.TableId,
            Tablet,
            _partitionPruner.NextPartitionKey,
            _isFaultTolerant);
    }

    private ScanRequest GetNextRowsRequest()
    {
        var request = new ScanRequestPB
        {
            ScannerId = _scannerId,
            CallSeqId = _sequenceId,
            BatchSizeBytes = (uint)_batchSizeBytes
        };

        return new ScanRequest(
            ScanRequestState.Next,
            request,
            _schema,
            _replicaSelection,
            _table.TableId,
            Tablet,
            _partitionPruner.NextPartitionKey,
            _isFaultTolerant);
    }

    private ScanRequest GetCloseRequest()
    {
        var request = new ScanRequestPB
        {
            ScannerId = _scannerId,
            BatchSizeBytes = 0,
            CloseScanner = true
        };

        return new ScanRequest(
            ScanRequestState.Closing,
            request,
            _schema,
            _replicaSelection,
            _table.TableId,
            Tablet,
            _partitionPruner.NextPartitionKey,
            _isFaultTolerant);
    }

    private KeepAliveRequest GetKeepAliveRequest()
    {
        return new KeepAliveRequest(
            _scannerId,
            _replicaSelection,
            _table.TableId,
            Tablet,
            _partitionPruner.NextPartitionKey);
    }

    private void ScanFinished()
    {
        Partition partition = Tablet!.Partition;
        _partitionPruner.RemovePartitionKeyRange(partition.PartitionKeyEnd);
        // Stop scanning if we have scanned until or past the end partition key, or
        // if we have fulfilled the limit.
        if (!_partitionPruner.HasMorePartitionKeyRanges || _numRowsReturned >= _limit)
        {
            _closed = true; // The scanner is closed on the other side at this point.
            return;
        }

        _scannerId = ByteString.Empty;
        _sequenceId = 0;
        _lastPrimaryKey = ByteString.Empty;
        Invalidate();
    }

    /// <summary>
    /// Invalidates this scanner and makes it assume it's no longer opened.
    /// When a TabletServer goes away while we're scanning it, or some other type
    /// of access problem happens, this method should be called so that the
    /// scanner will have to re-locate the TabletServer and re-open itself.
    /// </summary>
    private void Invalidate()
    {
        Tablet = null;
    }

    private void ClearCurrent()
    {
        var current = Current;
        if (current is not null)
        {
            Current = null!;
            current.Invalidate();
        }
    }
}
