using System;
using System.Collections.Generic;
using System.Threading;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Scanner;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client;

public class KuduScanner : IAsyncEnumerable<ResultSet>
{
    private readonly ILogger _logger;
    private readonly KuduClient _client;
    private readonly KuduTable _table;
    private readonly List<ColumnSchemaPB> _projectedColumnsPb;
    private readonly Dictionary<string, KuduPredicate> _predicates;

    private readonly OrderModePB _orderMode;
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
        List<string>? projectedColumnNames,
        List<int>? projectedColumnIndexes,
        Dictionary<string, KuduPredicate> predicates,
        ReadMode readMode,
        ReplicaSelection replicaSelection,
        bool isFaultTolerant,
        int batchSizeBytes,
        long limit,
        bool cacheBlocks,
        long startTimestamp,
        long htTimestamp,
        byte[] lowerBoundPrimaryKey,
        byte[] upperBoundPrimaryKey,
        byte[] lowerBoundPartitionKey,
        byte[] upperBoundPartitionKey)
    {
        if (limit <= 0)
        {
            throw new ArgumentException($"Need a strictly positive number for the limit, got {limit}");
        }

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
        _predicates = predicates;
        ReadMode = readMode;
        ReplicaSelection = replicaSelection;
        _isFaultTolerant = isFaultTolerant;
        _limit = limit;
        CacheBlocks = cacheBlocks;
        _startTimestamp = startTimestamp;
        _htTimestamp = htTimestamp;
        _lowerBoundPrimaryKey = lowerBoundPrimaryKey;
        _upperBoundPrimaryKey = upperBoundPrimaryKey;
        _lowerBoundPartitionKey = lowerBoundPartitionKey;
        _upperBoundPartitionKey = upperBoundPartitionKey;

        // Add the IS_DELETED column for diff scans.
        bool includeDeletedColumn = startTimestamp != KuduClient.NoTimestamp;

        ProjectionSchema = GenerateProjectionSchema(
            table.Schema,
            projectedColumnNames,
            projectedColumnIndexes,
            includeDeletedColumn);

        _projectedColumnsPb = ToColumnSchemaPbs(ProjectionSchema);
        BatchSizeBytes = GetBatchSizeEstimate(batchSizeBytes);
    }

    /// <summary>
    /// The maximum number of rows that this scanner was configured
    /// to return.
    /// </summary>
    public long? Limit => _limit > 0 ? _limit : null;

    public KuduScanEnumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var partitionPruner = PartitionPruner.Create(
            _table.Schema,
            _table.PartitionSchema,
            _predicates,
            _lowerBoundPrimaryKey,
            _upperBoundPrimaryKey,
            _lowerBoundPartitionKey,
            _upperBoundPartitionKey);

        return new KuduScanEnumerator(
            _logger,
            _client,
            _table,
            _projectedColumnsPb,
            ProjectionSchema,
            _orderMode,
            ReadMode,
            ReplicaSelection,
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

    IAsyncEnumerator<ResultSet> IAsyncEnumerable<ResultSet>.GetAsyncEnumerator(CancellationToken cancellationToken)
    {
        return GetAsyncEnumerator(cancellationToken);
    }

    private static int GetBatchSizeEstimate(int batchSize)
    {
        // Try to avoid spilling a small amount of data into the next
        // sized ArrayPool bucket. There is overhead from 2 places:
        // 1) ScanResponsePB will be stored in the buffer.
        // 2) Batch size is a hint; Kudu may return slightly more data.

        // The default batch size is 8MB. Due to the small amount of
        // overhead listed above we would likely end up renting a 16MB
        // buffer from ArrayPool, wasting about half of that. Instead,
        // slightly reduce the batch size so everything is likely to fit
        // in 8MB.

        const int overhead = 4096;

        // Optimize for the default case of 8MB.
        if (batchSize == 1024 * 1024 * 8)
        {
            return batchSize - overhead;
        }

        // TODO: Optimize this for .NET 6.
        return batchSize;
    }

    private static KuduSchema GenerateProjectionSchema(
        KuduSchema schema,
        List<string>? projectedColumnNames,
        List<int>? projectedColumnIndexes,
        bool includeDeletedColumn)
    {
        var numColumns = projectedColumnNames?.Count
            ?? projectedColumnIndexes?.Count
            ?? schema.Columns.Count;

        if (includeDeletedColumn)
            numColumns++;

        // Map the column names to actual columns in the table schema.
        // If the user set this to 'null', we scan all columns.
        var columns = new List<ColumnSchema>(numColumns);
        if (projectedColumnNames is not null)
        {
            foreach (string columnName in projectedColumnNames)
            {
                var columnSchema = schema.GetColumn(columnName);
                columns.Add(columnSchema);
            }
        }
        else if (projectedColumnIndexes is not null)
        {
            foreach (int columnIndex in projectedColumnIndexes)
            {
                var columnSchema = schema.GetColumn(columnIndex);
                columns.Add(columnSchema);
            }
        }
        else
        {
            columns.AddRange(schema.Columns);
        }

        int isDeletedIndex = -1;
        if (includeDeletedColumn)
        {
            var deletedColumn = GenerateIsDeletedColumn(schema);
            columns.Add(deletedColumn);
            isDeletedIndex = columns.Count - 1;
        }

        return new KuduSchema(columns, isDeletedIndex);
    }

    private static List<ColumnSchemaPB> ToColumnSchemaPbs(KuduSchema schema)
    {
        var columnSchemas = schema.Columns;
        var deletedColumn = schema.HasIsDeleted
            ? schema.GetColumn(schema.IsDeletedIndex)
            : null;

        var columnSchemaPbs = new List<ColumnSchemaPB>(columnSchemas.Count);

        foreach (var columnSchema in columnSchemas)
        {
            var isDeleted = columnSchema == deletedColumn;
            var columnSchemaPb = ToColumnSchemaPb(columnSchema, isDeleted);
            columnSchemaPbs.Add(columnSchemaPb);
        }

        return columnSchemaPbs;
    }

    private static ColumnSchemaPB ToColumnSchemaPb(
        ColumnSchema columnSchema, bool isDeleted)
    {
        var type = isDeleted
            ? DataTypePB.IsDeleted
            : (DataTypePB)columnSchema.Type;

        var columnSchemaPb = new ColumnSchemaPB
        {
            Name = columnSchema.Name,
            Type = type,
            IsNullable = columnSchema.IsNullable,
            // Set isKey to false on the passed ColumnSchema.
            // This allows out of order key columns in projections.
            IsKey = false,
            TypeAttributes = columnSchema.TypeAttributes.ToTypeAttributesPb()
        };

        ProtobufHelper.CopyDefaultValueToPb(columnSchema, columnSchemaPb);

        return columnSchemaPb;
    }

    /// <summary>
    /// Generates and returns a ColumnSchema for the virtual IS_DELETED column.
    /// The column name is generated to ensure there is never a collision.
    /// </summary>
    /// <param name="schema">The table schema.</param>
    private static ColumnSchema GenerateIsDeletedColumn(KuduSchema schema)
    {
        var columnName = "is_deleted";

        // If the column already exists and we need to pick an alternate column name.
        while (schema.HasColumn(columnName))
        {
            columnName += "_";
        }

        return new ColumnSchema(
            columnName,
            KuduType.Bool,
            isKey: false,
            isNullable: false,
            defaultValue: false);
    }
}
