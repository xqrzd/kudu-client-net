using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public class KuduScanEnumerator<T> : IAsyncEnumerator<T>
    {
        private readonly ILogger _logger;
        private readonly KuduClient _client;
        private readonly KuduTable _table;
        private readonly IKuduScanParser<T> _parser;
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
        private byte[] _scannerId;
        private byte[] _lastPrimaryKey;

        /// <summary>
        /// The tabletSlice currently being scanned.
        /// If null, we haven't started scanning.
        /// If == DONE, then we're done scanning.
        /// Otherwise it contains a proper tabletSlice name, and we're currently scanning.
        /// </summary>
        internal RemoteTablet Tablet { get; set; }

        internal long SnapshotTimestamp { get; private set; }

        public T Current { get; private set; }

        public KuduScanEnumerator(
            ILogger logger,
            KuduClient client,
            KuduTable table,
            IKuduScanParser<T> parser,
            List<string> projectedColumnNames,
            List<int> projectedColumnIndexes,
            ReadMode readMode,
            bool isFaultTolerant,
            Dictionary<string, KuduPredicate> predicates,
            long limit,
            bool cacheBlocks,
            byte[] startPrimaryKey,
            byte[] endPrimaryKey,
            long startTimestamp,
            long htTimestamp,
            int? batchSizeBytes,
            PartitionPruner partitionPruner,
            ReplicaSelection replicaSelection,
            CancellationToken cancellationToken)
        {
            if (htTimestamp != KuduClient.NoTimestamp)
            {
                if (htTimestamp < 0)
                    throw new ArgumentException(
                        $"Need non-negative number for the scan, timestamp got {htTimestamp}");

                if (readMode != ReadMode.ReadAtSnapshot)
                    throw new ArgumentException(
                        "When specifying a HybridClock timestamp, the read mode needs to be set to READ_AT_SNAPSHOT");
            }
            if (startTimestamp != KuduClient.NoTimestamp)
            {
                if (htTimestamp < 0)
                    throw new ArgumentException(
                        "Must have both start and end timestamps for a diff scan");

                if (startTimestamp > htTimestamp)
                    throw new ArgumentException(
                        "Start timestamp must be less than or equal to end timestamp");
            }

            _isFaultTolerant = isFaultTolerant;
            if (isFaultTolerant)
            {
                if (readMode != ReadMode.ReadAtSnapshot)
                    throw new ArgumentException("Use of fault tolerance scanner " +
                        "requires the read mode to be set to READ_AT_SNAPSHOT");
                _orderMode = OrderModePB.Ordered;
            }
            else
                _orderMode = OrderModePB.Unordered;

            _logger = logger;
            _client = client;
            _table = table;
            _parser = parser;
            _partitionPruner = partitionPruner;
            _readMode = readMode;
            _predicates = predicates;
            _limit = limit;
            _cacheBlocks = cacheBlocks;
            _startPrimaryKey = startPrimaryKey ?? Array.Empty<byte>();
            _endPrimaryKey = endPrimaryKey ?? Array.Empty<byte>();
            _startTimestamp = startTimestamp;
            SnapshotTimestamp = htTimestamp;
            _lastPrimaryKey = Array.Empty<byte>();
            _cancellationToken = cancellationToken;
            // TODO: Register cancellation callback and cancel the scan.

            // Map the column names to actual columns in the table schema.
            // If the user set this to 'null', we scan all columns.
            _columns = new List<ColumnSchemaPB>();
            var columns = new List<ColumnSchema>();
            if (projectedColumnNames != null)
            {
                foreach (string columnName in projectedColumnNames)
                {
                    ColumnSchema originalColumn = table.Schema.GetColumn(columnName);
                    _columns.Add(ToColumnSchemaPb(originalColumn));
                    columns.Add(originalColumn);
                }
            }
            else if (projectedColumnIndexes != null)
            {
                foreach (int columnIndex in projectedColumnIndexes)
                {
                    ColumnSchema originalColumn = table.Schema.GetColumn(columnIndex);
                    _columns.Add(ToColumnSchemaPb(originalColumn));
                    columns.Add(originalColumn);
                }
            }
            else
            {
                foreach (ColumnSchema columnSchema in table.Schema.Columns)
                {
                    _columns.Add(ToColumnSchemaPb(columnSchema));
                    columns.Add(columnSchema);
                }
            }

            int isDeletedIndex = -1;

            // This is a diff scan so add the IS_DELETED column.
            if (startTimestamp != KuduClient.NoTimestamp)
            {
                var deletedColumn = GenerateIsDeletedColumn(table.Schema);
                _columns.Add(deletedColumn);

                var delColumn = new ColumnSchema(deletedColumn.Name, KuduType.Bool);
                columns.Add(delColumn);
                isDeletedIndex = columns.Count - 1;
            }

            _schema = new KuduSchema(columns, isDeletedIndex);
            _batchSizeBytes = batchSizeBytes ?? _schema.GetScannerBatchSizeEstimate();

            // If the partition pruner has pruned all partitions, then the scan can be
            // short circuited without contacting any tablet servers.
            if (!_partitionPruner.HasMorePartitionKeyRanges)
            {
                _closed = true;
            }

            _replicaSelection = replicaSelection;

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
                    using var rpc = GetCloseRequest();
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                    try
                    {
                        await _client.SendRpcAsync(rpc, cts.Token)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.ExceptionClosingScanner(ex);
                    }
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

        private async ValueTask<bool> OpenScannerAsync()
        {
            using var rpc = GetOpenRequest();
            ScanResponse<T> response;

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

                _scannerId = null;
                _sequenceId = 0;
                return await MoveNextAsync().ConfigureAwait(false);
            }
            catch
            {
                Invalidate();
                throw;
            }

            Tablet = rpc.Tablet;

            if (SnapshotTimestamp == KuduClient.NoTimestamp &&
                response.ScanTimestamp != KuduClient.NoTimestamp)
            {
                // If the server-assigned timestamp is present in the tablet
                // server's response, store it in the scanner. The stored value
                // is used for read operations in READ_AT_SNAPSHOT mode at
                // other tablet servers in the context of the same scan.
                SnapshotTimestamp = response.ScanTimestamp;
            }

            long lastPropagatedTimestamp = KuduClient.NoTimestamp;
            if (_readMode == ReadMode.ReadYourWrites &&
                response.ScanTimestamp != KuduClient.NoTimestamp)
            {
                // For READ_YOUR_WRITES mode, update the latest propagated timestamp
                // with the chosen snapshot timestamp sent back from the server, to
                // avoid unnecessarily wait for subsequent reads. Since as long as
                // the chosen snapshot timestamp of the next read is greater than
                // the previous one, the scan does not violate READ_YOUR_WRITES
                // session guarantees.
                lastPropagatedTimestamp = response.ScanTimestamp;
            }
            else if (response.PropagatedTimestamp != KuduClient.NoTimestamp)
            {
                // Otherwise we just use the propagated timestamp returned from
                // the server as the latest propagated timestamp.
                lastPropagatedTimestamp = response.PropagatedTimestamp;
            }
            if (lastPropagatedTimestamp != KuduClient.NoTimestamp)
            {
                _client.LastPropagatedTimestamp = lastPropagatedTimestamp;
            }

            if (_isFaultTolerant && response.LastPrimaryKey != null)
            {
                _lastPrimaryKey = response.LastPrimaryKey;
            }

            _numRowsReturned += response.NumRows;
            Current = response.Data;

            if (!response.HasMoreResults || response.ScannerId == null)
            {
                ScanFinished();

                if (response.NumRows == 0 && _partitionPruner.HasMorePartitionKeyRanges)
                {
                    return await MoveNextAsync().ConfigureAwait(false);
                }

                return response.NumRows > 0;
            }

            _scannerId = response.ScannerId;
            _sequenceId++;

            return response.NumRows > 0;
        }

        private async ValueTask<bool> ScanNextRowsAsync()
        {
            using var rpc = GetNextRowsRequest();
            ScanResponse<T> response;

            try
            {
                response = await _client.SendRpcAsync(rpc, _cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (FaultTolerantScannerExpiredException)
            {
                _logger.ScannerExpired(_scannerId, Tablet);

                // If encountered FaultTolerantScannerExpiredException, it means the
                // fault tolerant scanner on the server side expired. Therefore, open
                // a new scanner.
                Invalidate();

                _scannerId = null;
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

            _numRowsReturned += response.NumRows;
            Current = response.Data;

            if (!response.HasMoreResults)
            {
                // We're done scanning this tablet.
                ScanFinished();
                return response.NumRows > 0;
            }
            _sequenceId++;

            return response.NumRows > 0;
        }

        private ScanRequest<T> GetOpenRequest()
        {
            //checkScanningNotStarted();
            var request = new ScanRequestPB();

            var newRequest = request.NewScanRequest = new NewScanRequestPB
            {
                Limit = (ulong)(_limit - _numRowsReturned),
                OrderMode = _orderMode,
                CacheBlocks = _cacheBlocks,
                ReadMode = (ReadModePB)_readMode
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
                newRequest.StartPrimaryKey = _startPrimaryKey;

            if (_endPrimaryKey.Length > 0)
                newRequest.StopPrimaryKey = _endPrimaryKey;

            foreach (KuduPredicate predicate in _predicates.Values)
                newRequest.ColumnPredicates.Add(predicate.ToProtobuf());

            request.BatchSizeBytes = (uint)_batchSizeBytes;

            return new ScanRequest<T>(
                ScanRequestState.Opening,
                request,
                _schema,
                _parser,
                _replicaSelection,
                _table.TableId,
                Tablet,
                _partitionPruner.NextPartitionKey,
                _isFaultTolerant);
        }

        private ScanRequest<T> GetNextRowsRequest()
        {
            //checkScanningNotStarted();
            var request = new ScanRequestPB
            {
                ScannerId = _scannerId,
                CallSeqId = _sequenceId,
                BatchSizeBytes = (uint)_batchSizeBytes
            };

            return new ScanRequest<T>(
                ScanRequestState.Next,
                request,
                _schema,
                _parser,
                _replicaSelection,
                _table.TableId,
                Tablet,
                _partitionPruner.NextPartitionKey,
                _isFaultTolerant);
        }

        private ScanRequest<T> GetCloseRequest()
        {
            var request = new ScanRequestPB
            {
                ScannerId = _scannerId,
                BatchSizeBytes = 0,
                CloseScanner = true
            };

            return new ScanRequest<T>(
                ScanRequestState.Closing,
                request,
                _schema,
                _parser,
                _replicaSelection,
                _table.TableId,
                Tablet,
                _partitionPruner.NextPartitionKey,
                _isFaultTolerant);
        }

        private void ScanFinished()
        {
            Partition partition = Tablet.Partition;
            _partitionPruner.RemovePartitionKeyRange(partition.PartitionKeyEnd);
            // Stop scanning if we have scanned until or past the end partition key, or
            // if we have fulfilled the limit.
            if (!_partitionPruner.HasMorePartitionKeyRanges || _numRowsReturned >= _limit)
            {
                _closed = true; // The scanner is closed on the other side at this point.
                return;
            }

            _scannerId = null;
            _sequenceId = 0;
            _lastPrimaryKey = Array.Empty<byte>();
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
            if (current != null)
            {
                Current = default;
                (current as IDisposable)?.Dispose();
            }
        }

        /// <summary>
        /// Generates and returns a ColumnSchema for the virtual IS_DELETED column.
        /// The column name is generated to ensure there is never a collision.
        /// </summary>
        /// <param name="schema">The table schema.</param>
        private static ColumnSchemaPB GenerateIsDeletedColumn(KuduSchema schema)
        {
            var columnName = "is_deleted";

            // If the column already exists and we need to pick an alternate column name.
            while (schema.HasColumn(columnName))
            {
                columnName += "_";
            }

            return new ColumnSchemaPB
            {
                Name = columnName,
                Type = DataTypePB.IsDeleted,
                ReadDefaultValue = new byte[] { 0 },
                IsNullable = false,
                IsKey = false
            };
        }

        private static ColumnSchemaPB ToColumnSchemaPb(ColumnSchema columnSchema)
        {
            return new ColumnSchemaPB
            {
                Name = columnSchema.Name,
                Type = (DataTypePB)columnSchema.Type,
                IsNullable = columnSchema.IsNullable,
                // Set isKey to false on the passed ColumnSchema.
                // This allows out of order key columns in projections.
                IsKey = false,
                TypeAttributes = columnSchema.TypeAttributes == null ? null : new ColumnTypeAttributesPB
                {
                    Precision = columnSchema.TypeAttributes.Precision,
                    Scale = columnSchema.TypeAttributes.Scale
                }
            };
        }
    }
}
