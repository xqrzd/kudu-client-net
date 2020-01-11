using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;
using ProtoBuf;

namespace Knet.Kudu.Client
{
    public class KuduScanEnumerator : IAsyncEnumerator<ResultSet>
    {
        private readonly KuduClient _client;
        private readonly KuduTable _table;
        private readonly IKuduScanParser<ResultSet> _parser;
        private readonly List<ColumnSchemaPB> _columns;
        private readonly Schema _schema;
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
        private long _htTimestamp;
        private uint _sequenceId;
        private byte[] _scannerId;
        private byte[] _lastPrimaryKey;

        /// <summary>
        /// The tabletSlice currently being scanned.
        /// If null, we haven't started scanning.
        /// If == DONE, then we're done scanning.
        /// Otherwise it contains a proper tabletSlice name, and we're currently scanning.
        /// </summary>
        private RemoteTablet _tablet;

        public ResultSet Current { get; private set; }

        public KuduScanEnumerator(
            KuduClient client,
            KuduTable table,
            IKuduScanParser<ResultSet> parser,
            List<string> projectedNames,
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
                    throw new ArgumentOutOfRangeException(
                        $"Need non-negative number for the scan, timestamp got {htTimestamp}");

                if (readMode != ReadMode.ReadAtSnapshot)
                    throw new ArgumentOutOfRangeException(
                        "When specifying a HybridClock timestamp, the read mode needs to be set to READ_AT_SNAPSHOT");
            }
            if (startTimestamp != KuduClient.NoTimestamp)
            {
                if (htTimestamp < 0)
                    throw new ArgumentOutOfRangeException(
                        "Must have both start and end timestamps for a diff scan");

                if (startTimestamp > htTimestamp)
                    throw new ArgumentOutOfRangeException(
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
            _htTimestamp = htTimestamp;
            _lastPrimaryKey = Array.Empty<byte>();
            _cancellationToken = cancellationToken;

            // Map the column names to actual columns in the table schema.
            // If the user set this to 'null', we scan all columns.
            _columns = new List<ColumnSchemaPB>();
            var columns = new List<ColumnSchema>();
            if (projectedNames != null)
            {
                foreach (string columnName in projectedNames)
                {
                    ColumnSchema originalColumn = table.Schema.GetColumn(columnName);
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
            _schema = new Schema(columns);
            _batchSizeBytes = batchSizeBytes ?? GetScannerBatchSizeEstimate(_schema);
            // This is a diff scan so add the IS_DELETED column.
            if (startTimestamp != KuduClient.NoTimestamp)
            {
                _columns.Add(GenerateIsDeletedColumn(table.Schema));
            }

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

        /// <summary>
        /// Generates and returns a ColumnSchema for the virtual IS_DELETED column.
        /// The column name is generated to ensure there is never a collision.
        /// </summary>
        /// <param name="schema">The table schema.</param>
        private static ColumnSchemaPB GenerateIsDeletedColumn(Schema schema)
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

        public async ValueTask DisposeAsync()
        {
            if (Current != null)
            {
                Current.Dispose();
                Current = null;
            }

            if (!_closed)
            {
                // Getting a null tablet here without being in a closed state
                // means we were in between tablets.
                if (_tablet != null)
                {
                    using var rpc = GetCloseRequest();
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                    try
                    {
                        await _client.SendRpcToTabletAsync(rpc, cts.Token)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        // TODO: Log warning.
                        Console.WriteLine($"Error closing scanner: {ex}");
                    }
                }

                _closed = true;
                Invalidate();
            }
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            // TODO: Catch OperationCancelledException, and call DisposeAsync()
            // Only wait a small amount of time to cancel the scan on the server.

            if (Current != null)
            {
                Current.Dispose();
                Current = null;
            }

            if (_closed)
            {
                // We're already done scanning.
                return false;
            }
            else if (_tablet == null)
            {
                // We need to open the scanner first.
                using var rpc = GetOpenRequest();
                var resp = await _client.SendRpcToTabletAsync(rpc, _cancellationToken)
                    .ConfigureAwait(false);

                _tablet = rpc.Tablet;

                if (_htTimestamp == KuduClient.NoTimestamp &&
                    resp.ScanTimestamp != KuduClient.NoTimestamp)
                {
                    // If the server-assigned timestamp is present in the tablet
                    // server's response, store it in the scanner. The stored value
                    // is used for read operations in READ_AT_SNAPSHOT mode at
                    // other tablet servers in the context of the same scan.
                    _htTimestamp = resp.ScanTimestamp;
                }

                long lastPropagatedTimestamp = KuduClient.NoTimestamp;
                if (_readMode == ReadMode.ReadYourWrites &&
                    resp.ScanTimestamp != KuduClient.NoTimestamp)
                {
                    // For READ_YOUR_WRITES mode, update the latest propagated timestamp
                    // with the chosen snapshot timestamp sent back from the server, to
                    // avoid unnecessarily wait for subsequent reads. Since as long as
                    // the chosen snapshot timestamp of the next read is greater than
                    // the previous one, the scan does not violate READ_YOUR_WRITES
                    // session guarantees.
                    lastPropagatedTimestamp = resp.ScanTimestamp;
                }
                else if (resp.PropagatedTimestamp != KuduClient.NoTimestamp)
                {
                    // Otherwise we just use the propagated timestamp returned from
                    // the server as the latest propagated timestamp.
                    lastPropagatedTimestamp = resp.PropagatedTimestamp;
                }
                if (lastPropagatedTimestamp != KuduClient.NoTimestamp)
                {
                    _client.LastPropagatedTimestamp = lastPropagatedTimestamp;
                }

                if (_isFaultTolerant && resp.LastPrimaryKey != null)
                {
                    _lastPrimaryKey = resp.LastPrimaryKey;
                }

                _numRowsReturned += resp.NumRows;
                Current = resp.Data;

                if (!resp.HasMoreResults || resp.ScannerId == null)
                {
                    ScanFinished();
                    return resp.NumRows > 0;
                }

                _scannerId = resp.ScannerId;
                _sequenceId++;

                return resp.NumRows > 0;
            }
            else
            {
                using var rpc = GetNextRowsRequest();
                var resp = await _client.SendRpcToTabletAsync(rpc, _cancellationToken)
                    .ConfigureAwait(false);

                _tablet = rpc.Tablet;

                _numRowsReturned += resp.NumRows;
                Current = resp.Data;

                if (!resp.HasMoreResults)
                {  // We're done scanning this tablet.
                    ScanFinished();
                    return resp.NumRows > 0;
                }
                _sequenceId++;

                return resp.NumRows > 0;
            }
        }

        private ColumnSchemaPB ToColumnSchemaPb(ColumnSchema columnSchema)
        {
            // TODO: Move this to shared location.
            return new ColumnSchemaPB
            {
                Name = columnSchema.Name,
                Type = (DataTypePB)columnSchema.Type,
                IsNullable = columnSchema.IsNullable,
                // Se isKey to false on the passed ColumnSchema.
                // This allows out of order key columns in projections.
                IsKey = false,
                // TODO: Default values
                // TODO: Block size
                Encoding = (EncodingTypePB)columnSchema.Encoding,
                Compression = (CompressionTypePB)columnSchema.Compression,
                TypeAttributes = columnSchema.TypeAttributes == null ? null : new ColumnTypeAttributesPB
                {
                    Precision = columnSchema.TypeAttributes.Precision,
                    Scale = columnSchema.TypeAttributes.Scale
                }
                // TODO: Comment
            };
        }

        private ScanRequest GetOpenRequest()
        {
            //checkScanningNotStarted();
            return new ScanRequest(this, State.Opening);
        }

        private ScanRequest GetNextRowsRequest()
        {
            //checkScanningNotStarted();
            return new ScanRequest(this, State.Next);
        }

        private ScanRequest GetCloseRequest()
        {
            return new ScanRequest(this, State.Closing);
        }

        private void ScanFinished()
        {
            Partition partition = _tablet.Partition;
            _partitionPruner.RemovePartitionKeyRange(partition.PartitionKeyEnd);
            // Stop scanning if we have scanned until or past the end partition key, or
            // if we have fulfilled the limit.
            if (!_partitionPruner.HasMorePartitionKeyRanges || _numRowsReturned >= _limit)
            {
                _closed = true; // The scanner is closed on the other side at this point.
                return;
            }

            //Console.WriteLine($"Done scanning tablet {_tablet.TabletId} for partition {_tablet.Partition} with scanner id {BitConverter.ToString(_scannerId)}");

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
            _tablet = null;
        }

        private static int GetScannerBatchSizeEstimate(Schema schema)
        {
            if (schema.VarLengthColumnCount == 0)
            {
                // No variable length data, we can do an
                // exact ideal estimate here.
                return 1024 * 1024 - schema.RowSize;
            }
            else
            {
                // Assume everything evens out.
                // Most of the time it probably does.
                return 1024 * 1024;
            }
        }

        private class ScanRequest : KuduTabletRpc<ScanResponse<ResultSet>>, IDisposable
        {
            private readonly KuduScanEnumerator _scanner;
            private readonly IKuduScanParser<ResultSet> _parser;
            private readonly State _state;

            private ScanResponsePB _responsePB;

            public ScanRequest(KuduScanEnumerator scanner, State state)
            {
                _scanner = scanner;
                _parser = scanner._parser;
                _state = state;
                Tablet = scanner._tablet;
                TableId = _scanner._table.TableId;
                PartitionKey = _scanner._partitionPruner.NextPartitionKey;
                NeedsAuthzToken = true;
            }

            public override string MethodName => "Scan";

            // TODO: Required features

            public override ReplicaSelection ReplicaSelection => _scanner._replicaSelection;

            // TODO: Authz token

            public void Dispose()
            {
                _parser.Dispose();
            }

            public override void Serialize(Stream stream)
            {
                var request = new ScanRequestPB();

                if (_state == State.Opening)
                {
                    var newRequest = request.NewScanRequest = new NewScanRequestPB();
                    newRequest.Limit = (ulong)(_scanner._limit - _scanner._numRowsReturned);
                    newRequest.ProjectedColumns.AddRange(_scanner._columns);
                    newRequest.TabletId = Tablet.TabletId.ToUtf8ByteArray();
                    newRequest.OrderMode = _scanner._orderMode;
                    newRequest.CacheBlocks = _scanner._cacheBlocks;
                    // If the last propagated timestamp is set, send it with the scan.
                    // For READ_YOUR_WRITES scan, use the propagated timestamp from
                    // the scanner.
                    long timestamp;
                    if (_scanner._readMode == ReadMode.ReadYourWrites)
                    {
                        timestamp = _scanner._lowerBoundPropagationTimestamp;
                    }
                    else
                    {
                        timestamp = _scanner._client.LastPropagatedTimestamp;
                    }
                    if (timestamp != KuduClient.NoTimestamp)
                    {
                        newRequest.PropagatedTimestamp = (ulong)timestamp;
                    }
                    newRequest.ReadMode = (ReadModePB)_scanner._readMode;

                    // If the mode is set to read on snapshot set the snapshot timestamps.
                    if (_scanner._readMode == ReadMode.ReadAtSnapshot)
                    {
                        if (_scanner._htTimestamp != KuduClient.NoTimestamp)
                            newRequest.SnapTimestamp = (ulong)_scanner._htTimestamp;

                        if (_scanner._startTimestamp != KuduClient.NoTimestamp)
                            newRequest.SnapStartTimestamp = (ulong)_scanner._startTimestamp;
                    }

                    if (_scanner._isFaultTolerant)
                    {
                        if (_scanner._lastPrimaryKey.Length > 0)
                        {
                            newRequest.LastPrimaryKey = _scanner._lastPrimaryKey;
                        }
                    }

                    if (_scanner._startPrimaryKey.Length > 0)
                    {
                        newRequest.StartPrimaryKey = _scanner._startPrimaryKey;
                    }

                    if (_scanner._endPrimaryKey.Length > 0)
                    {
                        newRequest.StopPrimaryKey = _scanner._endPrimaryKey;
                    }

                    foreach (KuduPredicate predicate in _scanner._predicates.Values)
                    {
                        newRequest.ColumnPredicates.Add(predicate.ToProtobuf());
                    }
                    if (AuthzToken != null)
                    {
                        newRequest.AuthzToken = AuthzToken;
                    }
                    if (PropagatedTimestamp != KuduClient.NoTimestamp)
                    {
                        newRequest.PropagatedTimestamp = (ulong)PropagatedTimestamp;
                    }
                    request.BatchSizeBytes = (uint)_scanner._batchSizeBytes;
                }
                else if (_state == State.Next)
                {
                    request.ScannerId = _scanner._scannerId;
                    request.CallSeqId = _scanner._sequenceId;
                    request.BatchSizeBytes = (uint)_scanner._batchSizeBytes;
                }
                else if (_state == State.Closing)
                {
                    request.ScannerId = _scanner._scannerId;
                    request.BatchSizeBytes = 0;
                    request.CloseScanner = true;
                }

                Serializer.SerializeWithLengthPrefix(stream, request, PrefixStyle.Base128);
            }

            public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
            {
                var resp = Serializer.Deserialize<ScanResponsePB>(buffer);

                _responsePB = resp;
                Error = resp.Error;
            }

            public override ScanResponse<ResultSet> Output
            {
                get
                {
                    return new ScanResponse<ResultSet>(
                        _responsePB.ScannerId,
                        _parser.Output,
                        _responsePB.Data.NumRows,
                        _responsePB.HasMoreResults,
                        _responsePB.ShouldSerializeSnapTimestamp() ? (long)_responsePB.SnapTimestamp : KuduClient.NoTimestamp,
                        _responsePB.ShouldSerializePropagatedTimestamp() ? (long)_responsePB.PropagatedTimestamp : KuduClient.NoTimestamp,
                        _responsePB.LastPrimaryKey);
                }
            }

            public override void BeginProcessingSidecars(KuduSidecarOffsets sidecars)
            {
                _parser.BeginProcessingSidecars(_scanner._schema, _responsePB, sidecars);
            }

            public override void ParseSidecarSegment(ref SequenceReader<byte> reader)
            {
                _parser.ParseSidecarSegment(ref reader);
            }
        }

        private enum State
        {
            Opening,
            Next,
            Closing
        }
    }
}
