using System;
using System.Collections.Generic;
using System.Threading;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public class AlterTableBuilder
    {
        private readonly KuduTable _table;
        private readonly AlterTableRequestPB _request;

        internal bool Wait { get; private set; } = true;

        public AlterTableBuilder(KuduTable table)
        {
            _table = table;
            _request = new AlterTableRequestPB
            {
                Table = new TableIdentifierPB
                {
                    TableId = _table.SchemaPb.TableId
                }
            };
        }

        /// <summary>
        /// Change a table's name.
        /// </summary>
        /// <param name="newName">New table's name, must be used to check progress.</param>
        public AlterTableBuilder RenameTable(string newName)
        {
            _request.NewTableName = newName;
            return this;
        }

        /// <summary>
        /// True if the alter table operation includes an add or drop
        /// partition operation.
        /// </summary>
        internal bool HasAddDropRangePartitions => _request.Schema != null;

        internal string TableId => _table.TableId;

        internal TableIdentifierPB TableIdPb => _request.Table;

        /// <summary>
        /// Add a new column to the table. The column defaults to a
        /// nullable non-key column.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="type">The column type.</param>
        /// <param name="configure">A delegate to further configure the column.</param>
        public AlterTableBuilder AddColumn(
            string name, KuduType type, Action<ColumnBuilder> configure = null)
        {
            var columnBuilder = new ColumnBuilder(name, type);
            configure?.Invoke(columnBuilder);

            ColumnSchemaPB schema = columnBuilder;

            if (!schema.IsNullable && schema.ReadDefaultValue == null)
                throw new ArgumentException("A new non-null column must have a default value");

            if (schema.IsKey)
                throw new ArgumentException("Key columns cannot be added");

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AddColumn,
                AddColumn = new AlterTableRequestPB.AddColumn
                {
                    Schema = schema
                }
            });

            return this;
        }

        /// <summary>
        /// Drop a column.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        public AlterTableBuilder DropColumn(string name)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.DropColumn,
                DropColumn = new AlterTableRequestPB.DropColumn
                {
                    Name = name
                }
            });

            return this;
        }

        /// <summary>
        /// Change the name of a column.
        /// </summary>
        /// <param name="oldName">Old column's name, must exist.</param>
        /// <param name="newName">New name to use.</param>
        public AlterTableBuilder RenameColumn(string oldName, string newName)
        {
            // For backwards compatibility, this uses the RENAME_COLUMN step type.
            // Needed for Kudu 1.3, the oldest supported version.
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.RenameColumn,
                RenameColumn = new AlterTableRequestPB.RenameColumn
                {
                    OldName = oldName,
                    NewName = newName
                }
            });

            return this;
        }

        /// <summary>
        /// Remove the default value for a column.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        public AlterTableBuilder RemoveDefault(string name)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        RemoveDefault = true
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the default value for a column. `newDefault` must not be null or
        /// else throws.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="newDefault">The new default value.</param>
        public AlterTableBuilder ChangeDefault(string name, object newDefault)
        {
            if (newDefault == null)
            {
                throw new ArgumentException("newDefault cannot be null: " +
                    "use RemoveDefault to clear a default value");
            }

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        DefaultValue = KuduEncoder.EncodeDefaultValue(newDefault)
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the block size of a column's storage. A nonpositive value
        /// indicates a server-side default.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="blockSize">The new block size.</param>
        public AlterTableBuilder ChangeDesiredBlockSize(string name, int blockSize)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        BlockSize = blockSize
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the encoding used for a column.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="encoding">The new encoding.</param>
        public AlterTableBuilder ChangeEncoding(string name, EncodingType encoding)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        Encoding = (EncodingTypePB)encoding
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the compression used for a column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="compressionType">The new compression algorithm.</param>
        public AlterTableBuilder ChangeCompressionAlgorithm(
            string name, CompressionType compressionType)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        Compression = (CompressionTypePB)compressionType
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the comment for the column.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="comment">
        /// The new comment for the column, an empty comment means
        /// deleting an existing comment.
        /// </param>
        public AlterTableBuilder ChangeComment(string name, string comment)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        NewComment = comment
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the table's extra configuration properties.
        /// These configuration properties will be merged into existing configuration
        /// properties.
        /// </summary>
        /// <param name="extraConfig">The table's extra configuration properties.</param>
        public AlterTableBuilder AlterExtraConfigs(
            IEnumerable<KeyValuePair<string, string>> extraConfig)
        {
            foreach (var kvp in extraConfig)
                _request.NewExtraConfigs.Add(kvp.Key, kvp.Value);

            return this;
        }

        /// <summary>
        /// Add a range partition to the table with an inclusive lower bound and an
        /// exclusive upper bound.
        /// 
        /// If either row is empty, then that end of the range will be unbounded.
        /// If a range column is missing a value, the logical minimum value for that
        /// column type will be used as the default.
        /// 
        /// Multiple range partitions may be added as part of a single alter table
        /// transaction by calling this method multiple times. Added range partitions
        /// must not overlap with each other or any existing range partitions (unless
        /// the existing range partitions are dropped as part of the alter transaction
        /// first). The lower bound must be less than the upper bound.
        /// 
        /// This client will immediately be able to write and scan the new tablets when
        /// the alter table operation returns success, however other existing clients may
        /// have to wait for a timeout period to elapse before the tablets become visible.
        /// This period is configured by the master's 'table_locations_ttl_ms' flag, and
        /// defaults to 5 minutes.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the inclusive lower bound and the exclusive upper
        /// bound (in that order).
        /// </param>
        public AlterTableBuilder AddRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure)
        {
            return AddRangePartition(
                configure,
                null,
                RangePartitionBound.Inclusive,
                RangePartitionBound.Exclusive);
        }

        /// <summary>
        /// Add a range partition to the table with a lower bound and upper bound.
        /// 
        /// If either row is empty, then that end of the range will be unbounded.
        /// If a range column is missing a value, the logical minimum value for that
        /// column type will be used as the default.
        /// 
        /// Multiple range partitions may be added as part of a single alter table
        /// transaction by calling this method multiple times. Added range partitions
        /// must not overlap with each other or any existing range partitions (unless
        /// the existing range partitions are dropped as part of the alter transaction
        /// first). The lower bound must be less than the upper bound.
        /// 
        /// This client will immediately be able to write and scan the new tablets when
        /// the alter table operation returns success, however other existing clients may
        /// have to wait for a timeout period to elapse before the tablets become visible.
        /// This period is configured by the master's 'table_locations_ttl_ms' flag, and
        /// defaults to 5 minutes.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the lower bound and the upper bound (in that order).
        /// </param>
        /// <param name="lowerBoundType">The type of the lower bound.</param>
        /// <param name="upperBoundType">The type of the upper bound.</param>
        public AlterTableBuilder AddRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure,
            RangePartitionBound lowerBoundType,
            RangePartitionBound upperBoundType)
        {
            return AddRangePartition(
                configure,
                null,
                lowerBoundType,
                upperBoundType);
        }

        /// <summary>
        /// Add a range partition to the table with dimension label.
        /// 
        /// If either row is empty, then that end of the range will be unbounded.
        /// If a range column is missing a value, the logical minimum value for that
        /// column type will be used as the default.
        /// 
        /// Multiple range partitions may be added as part of a single alter table
        /// transaction by calling this method multiple times. Added range partitions
        /// must not overlap with each other or any existing range partitions (unless
        /// the existing range partitions are dropped as part of the alter transaction
        /// first). The lower bound must be less than the upper bound.
        /// 
        /// This client will immediately be able to write and scan the new tablets when
        /// the alter table operation returns success, however other existing clients may
        /// have to wait for a timeout period to elapse before the tablets become visible.
        /// This period is configured by the master's 'table_locations_ttl_ms' flag, and
        /// defaults to 5 minutes.
        /// 
        /// By default, the master will try to place newly created tablet replicas on
        /// tablet servers with a small number of tablet replicas. If the dimension label
        /// is provided, newly created replicas will be evenly distributed in the cluster
        /// based on the dimension label.In other words, the master will try to place newly
        /// created tablet replicas on tablet servers with a small number of tablet replicas
        /// belonging to this dimension label.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the lower bound and the upper bound (in that order).
        /// </param>
        /// <param name="dimensionLabel">The dimension label for the tablet to be created.</param>
        /// <param name="lowerBoundType">The type of the lower bound.</param>
        /// <param name="upperBoundType">The type of the upper bound.</param>
        public AlterTableBuilder AddRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure,
            string dimensionLabel,
            RangePartitionBound lowerBoundType,
            RangePartitionBound upperBoundType)
        {
            var lowerRowOp = lowerBoundType == RangePartitionBound.Inclusive ?
                RowOperation.RangeLowerBound :
                RowOperation.ExclusiveRangeLowerBound;

            var upperRowOp = upperBoundType == RangePartitionBound.Exclusive ?
                RowOperation.RangeUpperBound :
                RowOperation.InclusiveRangeUpperBound;

            var schema = _table.Schema;
            var lowerBoundRow = new PartialRowOperation(schema, lowerRowOp);
            var upperBoundRow = new PartialRowOperation(schema, upperRowOp);

            configure(lowerBoundRow, upperBoundRow);

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AddRangePartition,
                AddRangePartition = new AlterTableRequestPB.AddRangePartition
                {
                    DimensionLabel = dimensionLabel,
                    RangeBounds = ProtobufHelper.EncodeRowOperations(
                        lowerBoundRow, upperBoundRow)
                }
            });

            if (_request.Schema == null)
                _request.Schema = _table.SchemaPbNoIds.Schema;

            return this;
        }

        /// <summary>
        /// Add a range partition to the table with with an identical lower bound
        /// and upper bound.
        /// 
        /// Multiple range partitions may be added as part of a single alter table
        /// transaction by calling this method multiple times. Added range partitions
        /// must not overlap with each other or any existing range partitions (unless
        /// the existing range partitions are dropped as part of the alter transaction
        /// first).
        /// 
        /// This client will immediately be able to write and scan the new tablets when
        /// the alter table operation returns success, however other existing clients may
        /// have to wait for a timeout period to elapse before the tablets become visible.
        /// This period is configured by the master's 'table_locations_ttl_ms' flag, and
        /// defaults to 5 minutes.
        /// </summary>
        /// <param name="configure">Delegate to configure the partition row.</param>
        public AlterTableBuilder AddRangePartition(
            Action<PartialRowOperation> configure)
        {
            var schema = _table.Schema;
            var lowerBoundRow = new PartialRowOperation(
                schema, RowOperation.RangeLowerBound);
            configure(lowerBoundRow);

            var upperBoundRow = new PartialRowOperation(
                lowerBoundRow, RowOperation.InclusiveRangeUpperBound);

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AddRangePartition,
                AddRangePartition = new AlterTableRequestPB.AddRangePartition
                {
                    RangeBounds = ProtobufHelper.EncodeRowOperations(
                        lowerBoundRow, upperBoundRow)
                }
            });

            if (_request.Schema == null)
                _request.Schema = _table.SchemaPbNoIds.Schema;

            return this;
        }

        /// <summary>
        /// Drop the range partition from the table with the specified inclusive lower
        /// bound and exclusive upper bound. The bounds must match exactly, and may not
        /// span multiple range partitions.
        /// 
        /// If either row is empty, then that end of the range will be unbounded. If a
        /// range column is missing a value, the logical minimum value for that column
        /// type will be used as the default.
        /// 
        /// Multiple range partitions may be dropped as part of a single alter table
        /// transaction by calling this method multiple times.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the lower bound and the upper bound (in that order).
        /// </param>
        public AlterTableBuilder DropRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure)
        {
            return DropRangePartition(
                configure,
                RangePartitionBound.Inclusive,
                RangePartitionBound.Exclusive);
        }

        /// <summary>
        /// Drop the range partition from the table with the specified lower bound and
        /// upper bound. The bounds must match exactly, and may not span multiple range
        /// partitions.
        /// 
        /// If either row is empty, then that end of the range will be unbounded. If a
        /// range column is missing a value, the logical minimum value for that column
        /// type will be used as the default.
        /// 
        /// Multiple range partitions may be dropped as part of a single alter table
        /// transaction by calling this method multiple times.
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure the lower bound and the upper bound (in that order).
        /// </param>
        /// <param name="lowerBoundType">The type of the lower bound.</param>
        /// <param name="upperBoundType">The type of the upper bound.</param>
        public AlterTableBuilder DropRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure,
            RangePartitionBound lowerBoundType,
            RangePartitionBound upperBoundType)
        {
            var lowerRowOp = lowerBoundType == RangePartitionBound.Inclusive ?
                RowOperation.RangeLowerBound :
                RowOperation.ExclusiveRangeLowerBound;

            var upperRowOp = upperBoundType == RangePartitionBound.Exclusive ?
                RowOperation.RangeUpperBound :
                RowOperation.InclusiveRangeUpperBound;

            var schema = _table.Schema;
            var lowerBoundRow = new PartialRowOperation(schema, lowerRowOp);
            var upperBoundRow = new PartialRowOperation(schema, upperRowOp);

            configure(lowerBoundRow, upperBoundRow);

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.DropRangePartition,
                DropRangePartition = new AlterTableRequestPB.DropRangePartition
                {
                    RangeBounds = ProtobufHelper.EncodeRowOperations(
                        lowerBoundRow, upperBoundRow)
                }
            });

            if (_request.Schema == null)
                _request.Schema = _table.SchemaPbNoIds.Schema;

            return this;
        }

        /// <summary>
        /// Drop the range partition from the table with with an identical lower
        /// bound and upper bound.
        /// 
        /// If a range column is missing a value, the logical minimum value for that
        /// column type will be used as the default.
        /// 
        /// Multiple range partitions may be dropped as part of a single alter table
        /// transaction by calling this method multiple times.
        /// </summary>
        /// <param name="configure">Delegate to configure the partition row.</param>
        public AlterTableBuilder DropRangePartition(
            Action<PartialRowOperation> configure)
        {
            var schema = _table.Schema;
            var lowerBoundRow = new PartialRowOperation(
                schema, RowOperation.RangeLowerBound);
            configure(lowerBoundRow);

            var upperBoundRow = new PartialRowOperation(
                lowerBoundRow, RowOperation.InclusiveRangeUpperBound);

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.DropRangePartition,
                DropRangePartition = new AlterTableRequestPB.DropRangePartition
                {
                    RangeBounds = ProtobufHelper.EncodeRowOperations(
                        lowerBoundRow, upperBoundRow)
                }
            });

            if (_request.Schema == null)
                _request.Schema = _table.SchemaPbNoIds.Schema;

            return this;
        }

        /// <summary>
        /// Whether to wait for the table to be fully altered before this alter
        /// operation is considered to be finished.
        /// 
        /// If false, the alter will finish quickly, but a subsequent
        /// <see cref="KuduClient.OpenTableAsync(string, CancellationToken)"/>
        /// may return a <see cref="KuduTable"/> with an out-of-date schema.
        /// 
        /// If true, the alter will take longer, but the very next schema is
        /// guaranteed to be up-to-date.
        /// 
        /// If not provided, defaults to true.
        /// </summary>
        /// <param name="wait">Whether to wait for the table to be fully altered.</param>
        public AlterTableBuilder SetWait(bool wait)
        {
            Wait = wait;
            return this;
        }

        public static implicit operator AlterTableRequestPB(AlterTableBuilder builder) => builder._request;
    }
}
