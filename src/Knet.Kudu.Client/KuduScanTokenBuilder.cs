using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Client;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public class KuduScanTokenBuilder : AbstractKuduScannerBuilder<KuduScanTokenBuilder>
    {
        // By default, a scan token is created for each tablet to be scanned.
        protected long SplitSizeBytes = -1;
        protected int FetchTabletsPerRangeLookup = 1000;

        public KuduScanTokenBuilder(KuduClient client, KuduTable table)
            : base(client, table)
        {
        }

        /// <summary>
        /// Sets the data size of key range. It is used to split tablet's primary key
        /// range into smaller ranges. The split doesn't change the layout of the
        /// tablet. This is a hint: The tablet server may return the size of key range
        /// larger or smaller than this value. If unset or less than or equal to 0, the
        /// key range includes all the data of the tablet.
        /// </summary>
        /// <param name="splitSizeBytes">The data size of key range.</param>
        public KuduScanTokenBuilder SetSplitSizeBytes(long splitSizeBytes)
        {
            SplitSizeBytes = splitSizeBytes;
            return this;
        }

        /// <summary>
        /// The number of tablets to fetch from the master when looking up a range of
        /// tablets.
        /// </summary>
        /// <param name="fetchTabletsPerRangeLookup">
        /// Number of tablets to fetch per range lookup.
        /// </param>
        public KuduScanTokenBuilder SetFetchTabletsPerRangeLookup(
            int fetchTabletsPerRangeLookup)
        {
            FetchTabletsPerRangeLookup = fetchTabletsPerRangeLookup;
            return this;
        }

        public async ValueTask<List<KuduScanToken>> BuildAsync(
            CancellationToken cancellationToken = default)
        {
            if (LowerBoundPartitionKey.Length != 0 ||
                UpperBoundPartitionKey.Length != 0)
            {
                throw new ArgumentException(
                    "Partition key bounds may not be set on KuduScanTokenBuilder");
            }

            // If the scan is short-circuitable, then return no tokens.
            foreach (var predicate in Predicates.Values)
            {
                if (predicate.Type == PredicateType.None)
                {
                    return new List<KuduScanToken>();
                }
            }

            var proto = new ScanTokenPB
            {
                TableId = Table.TableId,
                TableName = Table.TableName
            };

            // Map the column names or indices to actual columns in the table schema.
            // If the user did not set either projection, then scan all columns.
            var schema = Table.Schema;
            if (ProjectedColumnNames != null)
            {
                foreach (var columnName in ProjectedColumnNames)
                {
                    int columnIndex = schema.GetColumnIndex(columnName);
                    var columnSchema = Table.SchemaPb.Schema.Columns[columnIndex];

                    proto.ProjectedColumns.Add(columnSchema);
                }
            }
            else if (ProjectedColumnIndexes != null)
            {
                foreach (var columnIndex in ProjectedColumnIndexes)
                {
                    var columnSchema = Table.SchemaPb.Schema.Columns[columnIndex];

                    proto.ProjectedColumns.Add(columnSchema);
                }
            }
            else
            {
                proto.ProjectedColumns.AddRange(Table.SchemaPb.Schema.Columns);
            }

            foreach (var predicate in Predicates.Values)
            {
                proto.ColumnPredicates.Add(predicate.ToProtobuf());
            }

            if (LowerBoundPrimaryKey.Length > 0)
                proto.LowerBoundPrimaryKey = LowerBoundPrimaryKey;

            if (UpperBoundPrimaryKey.Length > 0)
                proto.UpperBoundPrimaryKey = UpperBoundPrimaryKey;

            proto.Limit = (ulong)Limit;
            proto.ReadMode = (ReadModePB)ReadMode;
            proto.ReplicaSelection = (ReplicaSelectionPB)ReplicaSelection;

            // If the last propagated timestamp is set send it with the scan.
            long lastPropagatedTimestamp = Client.LastPropagatedTimestamp;
            if (lastPropagatedTimestamp != KuduClient.NoTimestamp)
            {
                proto.PropagatedTimestamp = (ulong)lastPropagatedTimestamp;
            }

            // If the mode is set to read on snapshot set the snapshot timestamps.
            if (ReadMode == ReadMode.ReadAtSnapshot)
            {
                if (HtTimestamp != KuduClient.NoTimestamp)
                {
                    proto.SnapTimestamp = (ulong)HtTimestamp;
                }
                if (StartTimestamp != KuduClient.NoTimestamp)
                {
                    proto.SnapStartTimestamp = (ulong)StartTimestamp;
                }
            }

            proto.CacheBlocks = (CacheBlocks);
            proto.FaultTolerant = (IsFaultTolerant);
            proto.BatchSizeBytes = (uint)(BatchSizeBytes ?? schema.GetScannerBatchSizeEstimate());
            // TODO:
            //proto.setScanRequestTimeoutMs(scanRequestTimeout);
            //proto.setKeepAlivePeriodMs(keepAlivePeriodMs);
            //proto.ScanRequestTimeoutMs = 30000;
            //proto.KeepAlivePeriodMs = 15000;

            var pruner = PartitionPruner.Create(
                schema,
                Table.PartitionSchema,
                Predicates,
                LowerBoundPrimaryKey,
                UpperBoundPrimaryKey,
                LowerBoundPartitionKey,
                UpperBoundPartitionKey);

            var keyRanges = new List<KeyRange>();

            while (pruner.HasMorePartitionKeyRanges)
            {
                var partitionRange = pruner.NextPartitionKeyRange;

                var newKeyRanges = await Client.GetTableKeyRangesAsync(
                    Table.TableId,
                    LowerBoundPrimaryKey,
                    UpperBoundPrimaryKey,
                    partitionRange.Lower.Length == 0 ? null : partitionRange.Lower,
                    partitionRange.Upper.Length == 0 ? null : partitionRange.Upper,
                    FetchTabletsPerRangeLookup,
                    SplitSizeBytes,
                    cancellationToken).ConfigureAwait(false);

                if (newKeyRanges.Count == 0)
                {
                    pruner.RemovePartitionKeyRange(partitionRange.Upper);
                }
                else
                {
                    pruner.RemovePartitionKeyRange(
                        newKeyRanges[newKeyRanges.Count - 1].PartitionKeyEnd);
                }

                keyRanges.AddRange(newKeyRanges);
            }

            var tokens = new List<KuduScanToken>(keyRanges.Count);

            foreach (var keyRange in keyRanges)
            {
                var token = proto.Clone();

                token.LowerBoundPartitionKey = keyRange.PartitionKeyStart;
                token.UpperBoundPartitionKey = keyRange.PartitionKeyEnd;

                byte[] primaryKeyStart = keyRange.PrimaryKeyStart;

                if (primaryKeyStart != null && primaryKeyStart.Length > 0)
                    token.LowerBoundPrimaryKey = primaryKeyStart;

                byte[] primaryKeyEnd = keyRange.PrimaryKeyEnd;

                if (primaryKeyEnd != null && primaryKeyEnd.Length > 0)
                    token.UpperBoundPrimaryKey = primaryKeyEnd;

                tokens.Add(new KuduScanToken(keyRange, token));
            }

            return tokens;
        }
    }
}

namespace Knet.Kudu.Client.Protocol.Client
{
    public partial class ScanTokenPB
    {
        public ScanTokenPB Clone()
        {
            return (ScanTokenPB)MemberwiseClone();
        }
    }
}
