using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Client;
using Knet.Kudu.Client.Protobuf.Consensus;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;
using static Knet.Kudu.Client.Protobuf.Client.TabletMetadataPB.Types;

namespace Knet.Kudu.Client
{
    public class KuduScanTokenBuilder : AbstractKuduScannerBuilder<KuduScanTokenBuilder>
    {
        private readonly ISystemClock _systemClock;
        // By default, a scan token is created for each tablet to be scanned.
        private long _splitSizeBytes = -1;
        private int _fetchTabletsPerRangeLookup = 1000;
        private bool _includeTableMetadata = true;
        private bool _includeTabletMetadata = true;

        public KuduScanTokenBuilder(KuduClient client, KuduTable table, ISystemClock systemClock)
            : base(client, table)
        {
            _systemClock = systemClock;
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
            _splitSizeBytes = splitSizeBytes;
            return this;
        }

        /// <summary>
        /// The number of tablets to fetch from the master when looking up a range of tablets.
        /// </summary>
        /// <param name="fetchTabletsPerRangeLookup">
        /// Number of tablets to fetch per range lookup.
        /// </param>
        public KuduScanTokenBuilder SetFetchTabletsPerRangeLookup(int fetchTabletsPerRangeLookup)
        {
            _fetchTabletsPerRangeLookup = fetchTabletsPerRangeLookup;
            return this;
        }

        /// <summary>
        /// If the table metadata is included on the scan token a GetTableSchema
        /// RPC call to the master can be avoided when deserializing each scan token
        /// into a scanner.
        /// </summary>
        /// <param name="includeMetadata">True, if table metadata should be included.</param>
        public KuduScanTokenBuilder IncludeTableMetadata(bool includeMetadata)
        {
            _includeTableMetadata = includeMetadata;
            return this;
        }

        /// <summary>
        /// If the tablet metadata is included on the scan token a GetTableLocations
        /// RPC call to the master can be avoided when scanning with a scanner constructed
        /// from a scan token.
        /// </summary>
        /// <param name="includeMetadata">True, if tablet metadata should be included.</param>
        public KuduScanTokenBuilder IncludeTabletMetadata(bool includeMetadata)
        {
            _includeTabletMetadata = includeMetadata;
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

            var proto = new ScanTokenPB();

            if (_includeTableMetadata)
            {
                // Set the table metadata so that a call to the master is not needed when
                // deserializing the token into a scanner.
                var tableMetadataPb = new TableMetadataPB
                {
                    TableId = Table.TableId,
                    TableName = Table.TableName,
                    NumReplicas = Table.NumReplicas,
                    Schema = Table.SchemaPb.Schema,
                    PartitionSchema = Table.SchemaPb.PartitionSchema
                };

                if (Table.Owner is not null)
                {
                    tableMetadataPb.Owner = Table.Owner;
                }

                // TODO: Set table comment

                if (Table.ExtraConfig.Count > 0)
                {
                    foreach (var kvp in Table.ExtraConfig)
                    {
                        tableMetadataPb.ExtraConfigs.Add(kvp.Key, kvp.Value);
                    }
                }

                proto.TableMetadata = tableMetadataPb;

                // Only include the authz token if the table metadata is included.
                // It is returned in the required GetTableSchema request otherwise.
                var authzToken = Client.GetAuthzToken(Table.TableId);
                if (authzToken is not null)
                {
                    proto.AuthzToken = authzToken;
                }
            }
            else
            {
                // If we add the table metadata, we don't need to set the old table id
                // and table name. It is expected that the creation and use of a scan token
                // will be on the same or compatible versions.
                proto.TableId = Table.TableId;
                proto.TableName = Table.TableName;
            }

            // Map the column names or indices to actual columns in the table schema.
            // If the user did not set either projection, then scan all columns.
            var schema = Table.Schema;
            if (_includeTableMetadata)
            {
                // If the table metadata is included, then the column indexes can be
                // used instead of duplicating the ColumnSchemaPBs in the serialized
                // scan token.
                if (ProjectedColumnNames is not null)
                {
                    proto.ProjectedColumnIdx.Capacity = ProjectedColumnNames.Count;

                    foreach (var columnName in ProjectedColumnNames)
                    {
                        var columnIndex = schema.GetColumnIndex(columnName);
                        proto.ProjectedColumnIdx.Add(columnIndex);
                    }
                }
                else if (ProjectedColumnIndexes is not null)
                {
                    proto.ProjectedColumnIdx.AddRange(ProjectedColumnIndexes);
                }
                else
                {
                    var numColumns = schema.Columns.Count;
                    proto.ProjectedColumnIdx.Capacity = numColumns;

                    for (int i = 0; i < numColumns; i++)
                    {
                        proto.ProjectedColumnIdx.Add(i);
                    }
                }
            }
            else
            {
                if (ProjectedColumnNames is not null)
                {
                    proto.ProjectedColumns.Capacity = ProjectedColumnNames.Count;

                    foreach (var columnName in ProjectedColumnNames)
                    {
                        int columnIndex = schema.GetColumnIndex(columnName);
                        var columnSchema = Table.SchemaPb.Schema.Columns[columnIndex];

                        proto.ProjectedColumns.Add(columnSchema);
                    }
                }
                else if (ProjectedColumnIndexes is not null)
                {
                    proto.ProjectedColumns.Capacity = ProjectedColumnIndexes.Count;

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
            }

            foreach (var predicate in Predicates.Values)
            {
                proto.ColumnPredicates.Add(predicate.ToProtobuf());
            }

            if (LowerBoundPrimaryKey.Length > 0)
                proto.LowerBoundPrimaryKey = UnsafeByteOperations.UnsafeWrap(LowerBoundPrimaryKey);

            if (UpperBoundPrimaryKey.Length > 0)
                proto.UpperBoundPrimaryKey = UnsafeByteOperations.UnsafeWrap(UpperBoundPrimaryKey);

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

            proto.CacheBlocks = CacheBlocks;
            proto.FaultTolerant = IsFaultTolerant;
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
                    _fetchTabletsPerRangeLookup,
                    _splitSizeBytes,
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
            var nowMillis = _systemClock.CurrentMilliseconds;

            foreach (var keyRange in keyRanges)
            {
                var token = proto.Clone();

                token.LowerBoundPartitionKey = UnsafeByteOperations.UnsafeWrap(keyRange.PartitionKeyStart);
                token.UpperBoundPartitionKey = UnsafeByteOperations.UnsafeWrap(keyRange.PartitionKeyEnd);

                var primaryKeyStart = keyRange.PrimaryKeyStart;
                if (primaryKeyStart.Length > 0)
                    token.LowerBoundPrimaryKey = UnsafeByteOperations.UnsafeWrap(primaryKeyStart);

                var primaryKeyEnd = keyRange.PrimaryKeyEnd;
                if (primaryKeyEnd.Length > 0)
                    token.UpperBoundPrimaryKey = UnsafeByteOperations.UnsafeWrap(primaryKeyEnd);

                var tablet = keyRange.Tablet;

                // Set the tablet metadata so that a call to the master is not needed to
                // locate the tablet to scan when opening the scanner.
                if (_includeTabletMetadata)
                {
                    // TODO: It would be more efficient to pass the TTL in instead of
                    // looking it up again here.
                    var entry = Client.GetTableLocationEntry(
                        Table.TableId, tablet.Partition.PartitionKeyStart);

                    long ttl;

                    if (entry is not null &&
                        entry.IsCoveredRange &&
                        (ttl = entry.Expiration - nowMillis) > 0)
                    {
                        // Build the list of server and replica metadata.
                        var tabletServers = tablet.Servers;
                        var replicas = tablet.Replicas;
                        var numTabletServers = tabletServers.Count;

                        var tabletMetadataPb = new TabletMetadataPB
                        {
                            TabletId = tablet.TabletId,
                            Partition = ProtobufHelper.ToPartitionPb(tablet.Partition),
                            TtlMillis = (ulong)ttl
                        };

                        tabletMetadataPb.TabletServers.Capacity = numTabletServers;
                        tabletMetadataPb.Replicas.Capacity = numTabletServers;

                        for (int i = 0; i < numTabletServers; i++)
                        {
                            var serverInfo = tabletServers[i];
                            var replica = replicas[i];
                            var serverMetadataPb = ProtobufHelper.ToServerMetadataPb(serverInfo);

                            tabletMetadataPb.TabletServers.Add(serverMetadataPb);

                            var replicaMetadataPb = new ReplicaMetadataPB
                            {
                                TsIdx = (uint)i,
                                Role = (RaftPeerPB.Types.Role)replica.Role
                            };

                            if (replica.DimensionLabel is not null)
                            {
                                replicaMetadataPb.DimensionLabel = replica.DimensionLabel;
                            }

                            tabletMetadataPb.Replicas.Add(replicaMetadataPb);
                        }

                        token.TabletMetadata = tabletMetadataPb;
                    }
                }

                tokens.Add(new KuduScanToken(keyRange, token));
            }

            return tokens;
        }
    }
}
