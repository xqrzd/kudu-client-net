using System;
using System.Collections.Generic;
using System.Threading;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public class KuduScanner<T> : IAsyncEnumerable<T>
    {
        private static readonly ByteString _defaultDeletedColumnValue =
            UnsafeByteOperations.UnsafeWrap(new byte[] { 0 });

        private readonly ILogger _logger;
        private readonly KuduClient _client;
        private readonly KuduTable _table;
        private readonly IKuduScanParser<T> _parser;
        private readonly List<ColumnSchemaPB> _projectedColumnsPb;
        private readonly Dictionary<string, KuduPredicate> _predicates;

        private readonly OrderModePB _orderMode;
        private readonly RowDataFormat _rowDataFormat;
        private readonly bool _isFaultTolerant;
        private readonly long _limit;
        private readonly long _startTimestamp;
        private readonly long _htTimestamp;

        private readonly byte[] _lowerBoundPrimaryKey;
        private readonly byte[] _upperBoundPrimaryKey;
        private readonly byte[] _lowerBoundPartitionKey;
        private readonly byte[] _upperBoundPartitionKey;

        /// <summary>
        /// The projection schema of this scanner. If specific columns
        /// were not specified during scanner creation, the table schema
        /// is returned.
        /// </summary>
        public KuduSchema ProjectionSchema { get; }

        public ReplicaSelection ReplicaSelection { get; }

        public ReadMode ReadMode { get; }

        /// <summary>
        /// Whether data blocks will be cached when read from the files
        /// or discarded after use. Disable this to lower cache churn
        /// when doing large scans.
        /// </summary>
        public bool CacheBlocks { get; }

        /// <summary>
        /// The maximum number of bytes to send in the response.
        /// This is a hint, not a requirement: the server may send
        /// arbitrarily fewer or more bytes than requested.
        /// </summary>
        public int BatchSizeBytes { get; }

        public KuduScanner(
            ILogger logger,
            KuduClient client,
            KuduTable table,
            IKuduScanParser<T> parser,
            List<string> projectedColumnNames,
            List<int> projectedColumnIndexes,
            Dictionary<string, KuduPredicate> predicates,
            ReadMode readMode,
            ReplicaSelection replicaSelection,
            RowDataFormat rowDataFormat,
            bool isFaultTolerant,
            int? batchSizeBytes,
            long limit,
            bool cacheBlocks,
            long startTimestamp,
            long htTimestamp,
            byte[] lowerBoundPrimaryKey,
            byte[] upperBoundPrimaryKey,
            byte[] lowerBoundPartitionKey,
            byte[] upperBoundPartitionKey)
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
            _predicates = predicates;
            ReadMode = readMode;
            ReplicaSelection = replicaSelection;
            _rowDataFormat = rowDataFormat;
            _isFaultTolerant = isFaultTolerant;
            _limit = limit;
            CacheBlocks = cacheBlocks;
            _startTimestamp = startTimestamp;
            _htTimestamp = htTimestamp;
            _lowerBoundPrimaryKey = lowerBoundPrimaryKey;
            _upperBoundPrimaryKey = upperBoundPrimaryKey;
            _lowerBoundPartitionKey = lowerBoundPartitionKey;
            _upperBoundPartitionKey = upperBoundPartitionKey;

            // Map the column names to actual columns in the table schema.
            // If the user set this to 'null', we scan all columns.
            _projectedColumnsPb = new List<ColumnSchemaPB>();
            var columns = new List<ColumnSchema>();
            if (projectedColumnNames != null)
            {
                foreach (string columnName in projectedColumnNames)
                {
                    ColumnSchema originalColumn = table.Schema.GetColumn(columnName);
                    _projectedColumnsPb.Add(ToColumnSchemaPb(originalColumn));
                    columns.Add(originalColumn);
                }
            }
            else if (projectedColumnIndexes != null)
            {
                foreach (int columnIndex in projectedColumnIndexes)
                {
                    ColumnSchema originalColumn = table.Schema.GetColumn(columnIndex);
                    _projectedColumnsPb.Add(ToColumnSchemaPb(originalColumn));
                    columns.Add(originalColumn);
                }
            }
            else
            {
                foreach (ColumnSchema columnSchema in table.Schema.Columns)
                {
                    _projectedColumnsPb.Add(ToColumnSchemaPb(columnSchema));
                    columns.Add(columnSchema);
                }
            }

            int isDeletedIndex = -1;

            // This is a diff scan so add the IS_DELETED column.
            if (startTimestamp != KuduClient.NoTimestamp)
            {
                var deletedColumn = GenerateIsDeletedColumn(table.Schema);
                _projectedColumnsPb.Add(deletedColumn);

                var delColumn = new ColumnSchema(deletedColumn.Name, KuduType.Bool);
                columns.Add(delColumn);
                isDeletedIndex = columns.Count - 1;
            }

            ProjectionSchema = new KuduSchema(columns, isDeletedIndex);
            BatchSizeBytes = batchSizeBytes ?? ProjectionSchema.GetScannerBatchSizeEstimate();
        }

        /// <summary>
        /// The maximum number of rows that this scanner was configured
        /// to return.
        /// </summary>
        public long? Limit => _limit > 0 ? _limit : null;

        public KuduScanEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            var partitionPruner = PartitionPruner.Create(
                _table.Schema,
                _table.PartitionSchema,
                _predicates,
                _lowerBoundPrimaryKey,
                _upperBoundPrimaryKey,
                _lowerBoundPartitionKey,
                _upperBoundPartitionKey);

            return new KuduScanEnumerator<T>(
                _logger,
                _client,
                _table,
                _parser,
                _projectedColumnsPb,
                ProjectionSchema,
                _orderMode,
                ReadMode,
                ReplicaSelection,
                _rowDataFormat,
                _isFaultTolerant,
                _predicates,
                _limit,
                CacheBlocks,
                _lowerBoundPrimaryKey,
                _upperBoundPrimaryKey,
                _startTimestamp,
                _htTimestamp,
                BatchSizeBytes,
                partitionPruner,
                cancellationToken);
        }

        IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return GetAsyncEnumerator(cancellationToken);
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
                TypeAttributes = columnSchema.TypeAttributes.ToTypeAttributesPb()
            };
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
                ReadDefaultValue = _defaultDeletedColumnValue,
                IsNullable = false,
                IsKey = false
            };
        }
    }
}
