using System;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public class TableBuilder
    {
        private readonly List<PartialRowOperation> _splitRowsRangeBounds;

        internal CreateTableRequestPB CreateTableRequest;

        /// <summary>
        /// Creates a new table builder with the given table name.
        /// </summary>
        /// <param name="tableName">The table's name.</param>
        public TableBuilder(string tableName = null)
        {
            CreateTableRequest = new CreateTableRequestPB
            {
                Name = tableName,
                Schema = new SchemaPB(),
                PartitionSchema = new PartitionSchemaPB
                {
                    RangeSchema = new PartitionSchemaPB.RangeSchemaPB()
                },
                SplitRowsRangeBounds = new RowOperationsPB()
            };

            _splitRowsRangeBounds = new List<PartialRowOperation>();
        }

        /// <summary>
        /// Sets the name of the table.
        /// </summary>
        /// <param name="name">The table's name.</param>
        public TableBuilder SetTableName(string name)
        {
            CreateTableRequest.Name = name;
            return this;
        }

        /// <summary>
        /// Sets the number of replicas that each tablet will have. If not specified,
        /// it uses the server-side default which is usually 3 unless changed by an
        /// administrator.
        /// </summary>
        /// <param name="numReplicas">The number of replicas to use.</param>
        public TableBuilder SetNumReplicas(int numReplicas)
        {
            CreateTableRequest.NumReplicas = numReplicas;
            return this;
        }

        /// <summary>
        /// Set the table owner as the provided username in configured external
        /// catalogs such as the Hive Metastore. Overrides the default of the
        /// currently logged-in username or Kerberos principal.
        /// </summary>
        /// <param name="owner">
        /// The username to set as the table owner in external catalogs.
        /// </param>
        public TableBuilder SetOwner(string owner)
        {
            CreateTableRequest.Owner = owner;
            return this;
        }

        /// <summary>
        /// Sets the dimension label for all tablets created at table creation time.
        /// 
        /// By default, the master will try to place newly created tablet replicas on
        /// tablet servers with a small number of tablet replicas. If the dimension label
        /// is provided, newly created replicas will be evenly distributed in the cluster
        /// based on the dimension label. In other words, the master will try to place
        /// newly created tablet replicas on tablet servers with a small number of tablet
        /// replicas belonging to this dimension label.
        /// </summary>
        /// <param name="dimensionLabel">The dimension label for the tablet to be created.</param>
        public TableBuilder SetDimensionLabel(string dimensionLabel)
        {
            CreateTableRequest.DimensionLabel = dimensionLabel;
            return this;
        }

        /// <summary>
        /// Sets the table's extra configuration properties.
        /// </summary>
        /// <param name="extraConfig">The table's extra configuration properties.</param>
        public TableBuilder SetExtraConfigs(
            IEnumerable<KeyValuePair<string, string>> extraConfig)
        {
            foreach (var kvp in extraConfig)
                CreateTableRequest.ExtraConfigs.Add(kvp.Key, kvp.Value);

            return this;
        }

        /// <summary>
        /// Add a new column to the table. The column defaults to a
        /// nullable non-key column.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="type">The column type.</param>
        /// <param name="configure">A delegate to further configure the column.</param>
        public TableBuilder AddColumn(
            string name, KuduType type, Action<ColumnBuilder> configure = null)
        {
            var builder = new ColumnBuilder(name, type);
            configure?.Invoke(builder);
            var columnSchemaPb = builder.Build().ToColumnSchemaPb();
            CreateTableRequest.Schema.Columns.Add(columnSchemaPb);
            return this;
        }

        /// <summary>
        /// Add a set of hash partitions to the table.
        /// 
        /// Each column must be a part of the table's primary key, and an individual
        /// column may only appear in a single hash component.
        /// 
        /// For each set of hash partitions added to the table, the total number of
        /// table partitions is multiplied by the number of buckets. For example, if a
        /// table is created with 3 split rows, and two hash partitions with 4 and 5
        /// buckets respectively, the total number of table partitions will be 80
        /// (4 range partitions * 4 hash buckets * 5 hash buckets).
        /// </summary>
        /// <param name="buckets">The number of buckets to hash into.</param>
        /// <param name="columns">The columns to hash.</param>
        public TableBuilder AddHashPartitions(int buckets, params string[] columns)
        {
            return AddHashPartitions(buckets, 0, columns);
        }

        /// <summary>
        /// Add a set of hash partitions to the table.
        /// 
        /// Each column must be a part of the table's primary key, and an individual
        /// column may only appear in a single hash component.
        /// 
        /// For each set of hash partitions added to the table, the total number of
        /// table partitions is multiplied by the number of buckets. For example, if a
        /// table is created with 3 split rows, and two hash partitions with 4 and 5
        /// buckets respectively, the total number of table partitions will be 80
        /// (4 range partitions * 4 hash buckets * 5 hash buckets).
        /// 
        /// This constructor takes a seed value, which can be used to randomize the
        /// mapping of rows to hash buckets. Setting the seed may provide some
        /// amount of protection against denial of service attacks when the hashed
        /// columns contain user provided values.
        /// </summary>
        /// <param name="buckets">The number of buckets to hash into.</param>
        /// <param name="seed">A hash seed.</param>
        /// <param name="columns">The columns to hash.</param>
        public TableBuilder AddHashPartitions(int buckets, uint seed, params string[] columns)
        {
            var partition = new PartitionSchemaPB.HashBucketSchemaPB
            {
                NumBuckets = buckets,
                Seed = seed
            };

            foreach (var column in columns)
            {
                partition.Columns.Add(
                    new PartitionSchemaPB.ColumnIdentifierPB { Name = column });
            }

            CreateTableRequest.PartitionSchema.HashBucketSchemas.Add(partition);

            return this;
        }

        /// <summary>
        /// Set the columns on which the table will be range-partitioned.
        /// 
        /// Every column must be a part of the table's primary key. If not set,
        /// the table is range partitioned by the primary key columns with a single
        /// unbounded partition. If called with an empty set, the table will be
        /// created without range partitioning.
        /// </summary>
        /// <param name="columns">The range partitioned columns.</param>
        public TableBuilder SetRangePartitionColumns(params string[] columns)
        {
            var schemaColumns = CreateTableRequest.PartitionSchema.RangeSchema.Columns;

            foreach (var column in columns)
            {
                schemaColumns.Add(
                    new PartitionSchemaPB.ColumnIdentifierPB { Name = column });
            }

            return this;
        }

        /// <summary>
        /// Add a range partition to the table with an inclusive lower bound and an
        /// exclusive upper bound.
        /// 
        /// If either row is empty, then that end of the range will be unbounded. If a
        /// range column is missing a value, the logical minimum value for that column
        /// type will be used as the default.
        /// 
        /// Multiple range bounds may be added, but they must not overlap. All split
        /// rows must fall in one of the range bounds. The lower bound must be less
        /// than the upper bound.
        /// 
        /// If not provided, the table's range will be unbounded.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the inclusive lower bound and the exclusive upper
        /// bound (in that order).
        /// </param>
        public TableBuilder AddRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure)
        {
            return AddRangePartition(
                configure,
                RangePartitionBound.Inclusive,
                RangePartitionBound.Exclusive);
        }

        /// <summary>
        /// Add a range partition partition to the table with a lower bound and upper
        /// bound.
        /// 
        /// If either row is empty, then that end of the range will be unbounded. If a
        /// range column is missing a value, the logical minimum value for that column
        /// type will be used as the default.
        /// 
        /// Multiple range bounds may be added, but they must not overlap. All split
        /// rows must fall in one of the range bounds. The lower bound must be less
        /// than or equal to the upper bound.
        /// 
        /// If not provided, the table's range will be unbounded.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the lower bound and the upper bound (in that order).
        /// </param>
        /// <param name="lowerBoundType">The type of the lower bound.</param>
        /// <param name="upperBoundType">The type of the upper bound.</param>
        public TableBuilder AddRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure,
            RangePartitionBound lowerBoundType,
            RangePartitionBound upperBoundType)
        {
            // TODO: Rework this
            var columns = CreateTableRequest.Schema.Columns
                .Select(c => ColumnSchema.FromProtobuf(c))
                .ToList();

            var lowerRowOp = lowerBoundType == RangePartitionBound.Inclusive ?
                RowOperation.RangeLowerBound :
                RowOperation.ExclusiveRangeLowerBound;

            var upperRowOp = upperBoundType == RangePartitionBound.Exclusive ?
                RowOperation.RangeUpperBound :
                RowOperation.InclusiveRangeUpperBound;

            var schema = new KuduSchema(columns);
            var lowerBoundRow = new PartialRowOperation(schema, lowerRowOp);
            var upperBoundRow = new PartialRowOperation(schema, upperRowOp);

            configure(lowerBoundRow, upperBoundRow);

            _splitRowsRangeBounds.Add(lowerBoundRow);
            _splitRowsRangeBounds.Add(upperBoundRow);

            return this;
        }

        /// <summary>
        /// Add a range partition partition to the table with an identical lower
        /// bound and upper bound.
        /// 
        /// If arange column is missing a value, the logical minimum value for that
        /// column type will be used as the default.
        /// 
        /// Multiple range bounds may be added, but they must not overlap. All split
        /// rows must fall in one of the range bounds.
        /// 
        /// If not provided, the table's range will be unbounded.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the partition row.
        /// </param>
        public TableBuilder AddRangePartition(
            Action<PartialRowOperation> configure)
        {
            // TODO: Rework this
            var columns = CreateTableRequest.Schema.Columns
                .Select(c => ColumnSchema.FromProtobuf(c))
                .ToList();

            var schema = new KuduSchema(columns);
            var lowerBoundRow = new PartialRowOperation(
                schema, RowOperation.RangeLowerBound);
            configure(lowerBoundRow);

            var upperBoundRow = new PartialRowOperation(
                lowerBoundRow, RowOperation.InclusiveRangeUpperBound);

            _splitRowsRangeBounds.Add(lowerBoundRow);
            _splitRowsRangeBounds.Add(upperBoundRow);

            return this;
        }

        /// <summary>
        /// Add a range partition split. The split row must fall in a range partition,
        /// and causes the range partition to split into two contiguous range partitions.
        /// </summary>
        /// <param name="configure">A delegate to configure the split row.</param>
        public TableBuilder AddSplitRow(Action<PartialRowOperation> configure)
        {
            // TODO: Rework this
            var columns = CreateTableRequest.Schema.Columns
                .Select(c => ColumnSchema.FromProtobuf(c))
                .ToList();

            var schema = new KuduSchema(columns);
            var splitRow = new PartialRowOperation(schema, RowOperation.SplitRow);

            configure(splitRow);

            _splitRowsRangeBounds.Add(splitRow);

            return this;
        }

        public CreateTableRequestPB Build()
        {
            if (_splitRowsRangeBounds.Count > 0)
            {
                CreateTableRequest.SplitRowsRangeBounds =
                    ProtobufHelper.EncodeRowOperations(_splitRowsRangeBounds);
            }

            return CreateTableRequest;
        }

        public static implicit operator CreateTableRequestPB(TableBuilder builder) => builder.CreateTableRequest;
    }
}
