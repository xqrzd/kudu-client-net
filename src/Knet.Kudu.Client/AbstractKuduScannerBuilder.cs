using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client;

/// <summary>
/// Abstract class to extend in order to create builders for scanners.
/// </summary>
public abstract class AbstractKuduScannerBuilder<TBuilder>
    where TBuilder : AbstractKuduScannerBuilder<TBuilder>
{
    protected internal readonly KuduClient Client;
    protected internal readonly KuduTable Table;

    /// <summary>
    /// Map of column name to predicate.
    /// </summary>
    protected internal readonly Dictionary<string, KuduPredicate> Predicates;

    protected internal ReadMode ReadMode = ReadMode.ReadLatest;
    protected internal bool IsFaultTolerant = false;
    protected internal int BatchSizeBytes = 1024 * 1024 * 8; // 8MB
    protected internal long Limit = long.MaxValue;
    protected internal bool CacheBlocks = true;
    protected internal long StartTimestamp = KuduClient.NoTimestamp;
    protected internal long HtTimestamp = KuduClient.NoTimestamp;
    protected internal byte[] LowerBoundPrimaryKey = Array.Empty<byte>();
    protected internal byte[] UpperBoundPrimaryKey = Array.Empty<byte>();
    protected internal byte[] LowerBoundPartitionKey = Array.Empty<byte>();
    protected internal byte[] UpperBoundPartitionKey = Array.Empty<byte>();
    protected internal List<string>? ProjectedColumnNames;
    protected internal List<int>? ProjectedColumnIndexes;
    protected internal long ScanRequestTimeout; // TODO: Expose this, and expose as TimeSpan?
    protected internal ReplicaSelection ReplicaSelection = ReplicaSelection.LeaderOnly;

    public AbstractKuduScannerBuilder(KuduClient client, KuduTable table)
    {
        Client = client;
        Table = table;
        Predicates = new Dictionary<string, KuduPredicate>();
        ScanRequestTimeout = -1; // TODO: Pull this from the client.
    }

    /// <summary>
    /// Set which columns will be read by the Scanner.
    /// The default is to read all columns.
    /// </summary>
    /// <param name="columnNames">The names of columns to read.</param>
    public TBuilder SetProjectedColumns(IEnumerable<string> columnNames)
    {
        ProjectedColumnNames = columnNames.AsList();
        return (TBuilder)this;
    }

    /// <summary>
    /// Set which columns will be read by the Scanner.
    /// The default is to read all columns.
    /// </summary>
    /// <param name="columnNames">The names of columns to read.</param>
    public TBuilder SetProjectedColumns(params string[] columnNames)
    {
        ProjectedColumnNames = columnNames?.ToList();
        return (TBuilder)this;
    }

    /// <summary>
    /// Set which columns will be read by the Scanner.
    /// The default is to read all columns.
    /// </summary>
    /// <param name="columnIndexes">The indexes of columns to read.</param>
    public TBuilder SetProjectedColumns(IEnumerable<int> columnIndexes)
    {
        ProjectedColumnNames = null;
        ProjectedColumnIndexes = columnIndexes.AsList();
        return (TBuilder)this;
    }

    /// <summary>
    /// Set which columns will be read by the Scanner.
    /// The default is to read all columns.
    /// </summary>
    /// <param name="columnIndexes">The indexes of columns to read.</param>
    public TBuilder SetProjectedColumns(params int[] columnIndexes)
    {
        ProjectedColumnNames = null;
        ProjectedColumnIndexes = columnIndexes?.ToList();
        return (TBuilder)this;
    }

    /// <summary>
    /// Don't read any columns. Used for counting.
    /// </summary>
    public TBuilder SetEmptyProjection()
    {
        ProjectedColumnNames = new List<string>();
        return (TBuilder)this;
    }

    /// <summary>
    /// Read every column in the table. This is the default.
    /// </summary>
    public TBuilder SetFullProjection()
    {
        ProjectedColumnNames = null;
        ProjectedColumnIndexes = null;
        return (TBuilder)this;
    }

    /// <summary>
    /// Sets the read mode, the default is to read the latest values.
    /// </summary>
    /// <param name="readMode">A read mode for the scanner.</param>
    public TBuilder SetReadMode(ReadMode readMode)
    {
        ReadMode = readMode;
        return (TBuilder)this;
    }

    /// <summary>
    /// Sets the replica selection mechanism for this scanner.
    /// The default is to read from the currently known leader.
    /// </summary>
    /// <param name="replicaSelection">Replication selection mechanism to use.</param>
    public TBuilder SetReplicaSelection(ReplicaSelection replicaSelection)
    {
        ReplicaSelection = replicaSelection;
        return (TBuilder)this;
    }

    /// <summary>
    /// <para>
    /// Make scans resumable at another tablet server if current server fails if
    /// isFaultTolerant is true.
    /// </para>
    ///
    /// <para>
    /// Scans are by default non fault-tolerant, and scans will fail
    /// if scanning an individual tablet fails (for example, if a tablet server
    /// crashes in the middle of a tablet scan). If isFaultTolerant is set to true,
    /// scans will be resumed at another tablet server in the case of failure.
    /// </para>
    ///
    /// <para>
    /// Fault-tolerant scans typically have lower throughput than non
    /// fault-tolerant scans. Fault tolerant scans use READ_AT_SNAPSHOT read mode.
    /// If no snapshot timestamp is provided, the server will pick one.
    /// </para>
    /// </summary>
    /// <param name="isFaultTolerant">Indicates if scan is fault-tolerant.</param>
    public TBuilder SetFaultTolerant(bool isFaultTolerant)
    {
        IsFaultTolerant = isFaultTolerant;
        if (isFaultTolerant)
            ReadMode = ReadMode.ReadAtSnapshot;
        return (TBuilder)this;
    }

    /// <summary>
    /// <para>
    /// Sets the maximum number of bytes returned by the scanner, on each batch.
    /// The default is 8MB. Kudu may actually return more than this many bytes
    /// because it will not truncate a rowResult in the middle.
    /// </para>
    /// <para>
    /// To increase this beyond 8MB, the Kudu server must be configured with a
    /// larger --scanner_max_batch_size_bytes value.
    /// </para>
    /// <para>
    /// Note: This client may slightly adjust this value to help prevent spilling
    /// a small amount of data into a larger <see cref="ArrayPool{T}"/> bucket.
    /// </para>
    /// </summary>
    /// <param name="batchSizeBytes">A strictly positive number of bytes.</param>
    public TBuilder SetBatchSizeBytes(int batchSizeBytes)
    {
        BatchSizeBytes = batchSizeBytes;
        return (TBuilder)this;
    }

    /// <summary>
    /// Sets a limit on the number of rows that will be returned by the scanner.
    /// There's no limit by default.
    /// </summary>
    /// <param name="limit">A positive long.</param>
    public TBuilder SetLimit(long limit)
    {
        Limit = limit;
        return (TBuilder)this;
    }

    /// <summary>
    /// Sets the block caching policy for the scanner. If true, scanned data blocks will
    /// be cached in memory and made available for future scans. Enabled by default.
    /// </summary>
    /// <param name="cacheBlocks">Indicates if data blocks should be cached or not.</param>
    public TBuilder SetCacheBlocks(bool cacheBlocks)
    {
        CacheBlocks = cacheBlocks;
        return (TBuilder)this;
    }

    /// <summary>
    /// Sets a previously encoded HT timestamp as a snapshot timestamp.
    /// None is used by default. Requires that the ReadMode is READ_AT_SNAPSHOT.
    /// </summary>
    /// <param name="htTimestamp">A long representing a HybridTime-encoded timestamp.</param>
    public TBuilder SnapshotTimestampRaw(long htTimestamp)
    {
        HtTimestamp = htTimestamp;
        return (TBuilder)this;
    }

    /// <summary>
    /// Sets the timestamp the scan must be executed at, in microseconds since the Unix epoch.
    /// None is used by default. Requires that the ReadMode is READ_AT_SNAPSHOT.
    /// </summary>
    /// <param name="timestamp">A long representing an instant in microseconds since the unix epoch.</param>
    public TBuilder SnapshotTimestampMicros(long timestamp)
    {
        HtTimestamp = HybridTimeUtil.PhysicalAndLogicalToHtTimestamp(timestamp, 0);
        return (TBuilder)this;
    }

    /// <summary>
    /// Sets the start timestamp and end timestamp for a diff scan.
    /// The timestamps should be encoded HT timestamps.
    /// Additionally sets any other scan properties required by diff scans.
    /// </summary>
    /// <param name="startTimestamp">A long representing a HybridTime-encoded start timestamp.</param>
    /// <param name="endTimestamp">A long representing a HybridTime-encoded end timestamp.</param>
    public TBuilder DiffScan(long startTimestamp, long endTimestamp)
    {
        StartTimestamp = startTimestamp;
        HtTimestamp = endTimestamp;
        IsFaultTolerant = true;
        ReadMode = ReadMode.ReadAtSnapshot;
        return (TBuilder)this;
    }

    /// <summary>
    /// Add a lower bound (inclusive) primary key for the scan.
    /// If any bound is already added, this bound is intersected with that one.
    /// </summary>
    /// <param name="row">A partial row with specified key columns.</param>
    public TBuilder LowerBound(PartialRow row)
    {
        byte[] startPrimaryKey = KeyEncoder.EncodePrimaryKey(row);
        return LowerBoundRaw(startPrimaryKey);
    }

    /// <summary>
    /// Add a lower bound (inclusive) primary key for the scan.
    /// If any bound is already added, this bound is intersected with that one.
    /// </summary>
    /// <param name="startPrimaryKey">An encoded start key.</param>
    internal TBuilder LowerBoundRaw(byte[] startPrimaryKey)
    {
        if (LowerBoundPrimaryKey.Length == 0 ||
            startPrimaryKey.SequenceCompareTo(LowerBoundPrimaryKey) > 0)
        {
            LowerBoundPrimaryKey = startPrimaryKey;
        }
        return (TBuilder)this;
    }

    /// <summary>
    /// Add an upper bound (exclusive) primary key for the scan.
    /// If any bound is already added, this bound is intersected with that one.
    /// </summary>
    /// <param name="row">A partial row with specified key columns.</param>
    public TBuilder ExclusiveUpperBound(PartialRow row)
    {
        byte[] endPrimaryKey = KeyEncoder.EncodePrimaryKey(row);
        return ExclusiveUpperBoundRaw(endPrimaryKey);
    }

    /// <summary>
    /// Add an upper bound (exclusive) primary key for the scan.
    /// If any bound is already added, this bound is intersected with that one.
    /// </summary>
    /// <param name="endPrimaryKey">An encoded end key.</param>
    internal TBuilder ExclusiveUpperBoundRaw(byte[] endPrimaryKey)
    {
        if (UpperBoundPrimaryKey.Length == 0 ||
            endPrimaryKey.SequenceCompareTo(UpperBoundPrimaryKey) < 0)
        {
            UpperBoundPrimaryKey = endPrimaryKey;
        }
        return (TBuilder)this;
    }

    /// <summary>
    /// Set an encoded (inclusive) start partition key for the scan.
    /// </summary>
    /// <param name="partitionKey">The encoded partition key.</param>
    internal TBuilder LowerBoundPartitionKeyRaw(byte[] partitionKey)
    {
        if (partitionKey.SequenceCompareTo(LowerBoundPartitionKey) > 0)
            LowerBoundPartitionKey = partitionKey;

        return (TBuilder)this;
    }

    /// <summary>
    /// Set an encoded (exclusive) end partition key for the scan.
    /// </summary>
    /// <param name="partitionKey">The encoded partition key.</param>
    internal TBuilder ExclusiveUpperBoundPartitionKeyRaw(byte[] partitionKey)
    {
        if (UpperBoundPartitionKey.Length == 0 ||
            partitionKey.SequenceCompareTo(UpperBoundPartitionKey) < 0)
        {
            UpperBoundPartitionKey = partitionKey;
        }

        return (TBuilder)this;
    }

    /// <summary>
    /// Adds a predicate to the scan.
    /// </summary>
    /// <param name="predicate">The predicate to add.</param>
    public TBuilder AddPredicate(KuduPredicate predicate)
    {
        var column = predicate.Column;
        var columnName = column.Name;

        if (Predicates.TryGetValue(columnName, out var existing))
        {
            predicate = existing.Merge(predicate);
        }

        // KUDU-1652: Do not send an IS NOT NULL predicate to the server for a non-nullable column.
        if (!column.IsNullable && predicate.Type == PredicateType.IsNotNull)
        {
            return (TBuilder)this;
        }

        Predicates[columnName] = predicate;
        return (TBuilder)this;
    }
}
