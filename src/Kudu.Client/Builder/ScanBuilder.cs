using System.Collections.Generic;
using System.Linq;
using Kudu.Client.Util;

namespace Kudu.Client.Builder
{
    public class ScanBuilder
    {
        internal KuduClient Client { get; }

        internal KuduTable Table { get; }

        /// <summary>
        /// Map of column name to predicate.
        /// </summary>
        internal Dictionary<string, KuduPredicate> Predicates { get; }

        internal List<string> ProjectedColumns { get; private set; }

        internal ReadMode ReadMode { get; private set; } = ReadMode.ReadLatest;

        internal ReplicaSelection ReplicaSelection { get; private set; } = ReplicaSelection.LeaderOnly;

        internal int BatchSizeBytes { get; private set; } = 1024 * 1024;

        internal long Limit { get; private set; } = long.MaxValue;

        internal bool CacheBlocks { get; private set; } = true;

        public ScanBuilder(KuduClient client, KuduTable table)
        {
            Client = client;
            Table = table;
            Predicates = new Dictionary<string, KuduPredicate>();
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
