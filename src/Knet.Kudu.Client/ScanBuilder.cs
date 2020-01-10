using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public class ScanBuilder
    {
        internal readonly KuduClient Client;
        internal readonly KuduTable Table;

        /// <summary>
        /// Map of column name to predicate.
        /// </summary>
        internal readonly Dictionary<string, KuduPredicate> Predicates;

        internal ReadMode ReadMode = ReadMode.ReadLatest;
        internal bool IsFaultTolerant = false;
        internal int? BatchSizeBytes;
        internal long Limit = long.MaxValue;
        internal bool CacheBlocks = true;
        internal long StartTimestamp = -1; // Not currently exposed.
        internal long HtTimestamp = -1;
        internal byte[] LowerBoundPrimaryKey;
        internal byte[] UpperBoundPrimaryKey;
        internal byte[] LowerBoundPartitionKey; // Not currently exposed.
        internal byte[] UpperBoundPartitionKey; // Not currently exposed.
        internal List<string> ProjectedColumns;
        internal long ScanRequestTimeout; // TODO: Expose this, and expose as TimeSpan?
        internal ReplicaSelection ReplicaSelection = ReplicaSelection.LeaderOnly;

        public ScanBuilder(KuduClient client, KuduTable table)
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
        /// <param name="columns">The names of columns to read, or 'null' to read all columns.</param>
        public ScanBuilder SetProjectedColumns(IEnumerable<string> columns)
        {
            ProjectedColumns = columns.AsList();
            return this;
        }

        /// <summary>
        /// Set which columns will be read by the Scanner.
        /// The default is to read all columns.
        /// </summary>
        /// <param name="columns">The names of columns to read, or 'null' to read all columns.</param>
        public ScanBuilder SetProjectedColumns(params string[] columns)
        {
            ProjectedColumns = columns.ToList();
            return this;
        }

        /// <summary>
        /// Sets the read mode, the default is to read the latest values.
        /// </summary>
        /// <param name="readMode">A read mode for the scanner.</param>
        public ScanBuilder SetReadMode(ReadMode readMode)
        {
            ReadMode = readMode;
            return this;
        }

        /// <summary>
        /// Sets the replica selection mechanism for this scanner.
        /// The default is to read from the currently known leader.
        /// </summary>
        /// <param name="replicaSelection">Replication selection mechanism to use.</param>
        public ScanBuilder SetReplicaSelection(ReplicaSelection replicaSelection)
        {
            ReplicaSelection = replicaSelection;
            return this;
        }

        /// <summary>
        /// Make scans resumable at another tablet server if current server fails if
        /// isFaultTolerant is true.
        ///
        /// Scans are by default non fault-tolerant, and scans will fail
        /// if scanning an individual tablet fails (for example, if a tablet server
        /// crashes in the middle of a tablet scan). If isFaultTolerant is set to true,
        /// scans will be resumed at another tablet server in the case of failure.
        ///
        /// Fault-tolerant scans typically have lower throughput than non
        /// fault-tolerant scans. Fault tolerant scans use READ_AT_SNAPSHOT read mode.
        /// If no snapshot timestamp is provided, the server will pick one.
        /// </summary>
        /// <param name="isFaultTolerant">Indicates if scan is fault-tolerant.</param>
        public ScanBuilder SetFaultTolerant(bool isFaultTolerant)
        {
            IsFaultTolerant = isFaultTolerant;
            if (isFaultTolerant)
                ReadMode = ReadMode.ReadAtSnapshot;
            return this;
        }

        /// <summary>
        /// Sets the maximum number of bytes returned by the scanner, on each batch.
        /// The default is 1MB. Kudu may actually return more than this many bytes
        /// because it will not truncate a rowResult in the middle.
        /// </summary>
        /// <param name="batchSizeBytes">A strictly positive number of bytes.</param>
        public ScanBuilder SetBatchSizeBytes(int batchSizeBytes)
        {
            BatchSizeBytes = batchSizeBytes;
            return this;
        }

        /// <summary>
        /// Sets a limit on the number of rows that will be returned by the scanner.
        /// There's no limit by default.
        /// </summary>
        /// <param name="limit">A positive long.</param>
        public ScanBuilder SetLimit(long limit)
        {
            Limit = limit;
            return this;
        }

        /// <summary>
        /// Sets the block caching policy for the scanner. If true, scanned data blocks will
        /// be cached in memory and made available for future scans. Enabled by default.
        /// </summary>
        /// <param name="cacheBlocks">Indicates if data blocks should be cached or not.</param>
        public ScanBuilder SetCacheBlocks(bool cacheBlocks)
        {
            CacheBlocks = cacheBlocks;
            return this;
        }

        /// <summary>
        /// Sets the timestamp the scan must be executed at, in microseconds since the Unix epoch.
        /// None is used by default. Requires that the ReadMode is READ_AT_SNAPSHOT.
        /// </summary>
        /// <param name="timestamp">A long representing an instant in microseconds since the unix epoch.</param>
        public ScanBuilder SnapshotTimestampMicros(long timestamp)
        {
            HtTimestamp = HybridTimeUtil.PhysicalAndLogicalToHTTimestamp(timestamp, 0);
            return this;
        }

        /// <summary>
        /// Add a lower bound (inclusive) primary key for the scan.
        /// If any bound is already added, this bound is intersected with that one.
        /// </summary>
        /// <param name="row">A partial row with specified key columns.</param>
        public ScanBuilder LowerBound(PartialRow row)
        {
            byte[] startPrimaryKey = KeyEncoder.EncodePrimaryKey(row);

            if (LowerBoundPrimaryKey.Length == 0 ||
                startPrimaryKey.SequenceCompareTo(LowerBoundPrimaryKey) > 0)
            {
                LowerBoundPrimaryKey = startPrimaryKey;
            }
            return this;
        }

        /// <summary>
        /// Add an upper bound (exclusive) primary key for the scan.
        /// If any bound is already added, this bound is intersected with that one.
        /// </summary>
        /// <param name="row">A partial row with specified key columns.</param>
        public ScanBuilder ExclusiveUpperBound(PartialRow row)
        {
            byte[] endPrimaryKey = KeyEncoder.EncodePrimaryKey(row);

            if (UpperBoundPrimaryKey.Length == 0 ||
                endPrimaryKey.SequenceCompareTo(UpperBoundPrimaryKey) < 0)
            {
                UpperBoundPrimaryKey = endPrimaryKey;
            }
            return this;
        }

        /// <summary>
        /// Adds a predicate to the scan.
        /// </summary>
        /// <param name="predicate">The predicate to add.</param>
        public ScanBuilder AddPredicate(KuduPredicate predicate)
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
                return this;
            }

            Predicates[columnName] = predicate;
            return this;
        }

        public KuduScanner Build() => new KuduScanner(this);
    }
}
