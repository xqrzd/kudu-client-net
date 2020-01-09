using System;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Master;

namespace Knet.Kudu.Client.Builder
{
    public class TableBuilder
    {
        private readonly List<PartialRowOperation> _splitRowsRangeBounds;

        internal CreateTableRequestPB CreateTableRequest;

        public TableBuilder()
        {
            CreateTableRequest = new CreateTableRequestPB
            {
                Schema = new SchemaPB(),
                PartitionSchema = new PartitionSchemaPB
                {
                    RangeSchema = new PartitionSchemaPB.RangeSchemaPB()
                },
                SplitRowsRangeBounds = new RowOperationsPB()
            };

            _splitRowsRangeBounds = new List<PartialRowOperation>();
        }

        public TableBuilder SetTableName(string name)
        {
            CreateTableRequest.Name = name;
            return this;
        }

        public TableBuilder SetNumReplicas(int numReplicas)
        {
            CreateTableRequest.NumReplicas = numReplicas;
            return this;
        }

        public TableBuilder AddColumn(Action<ColumnBuilder> setup)
        {
            var column = new ColumnBuilder();
            setup(column);
            CreateTableRequest.Schema.Columns.Add(column);
            return this;
        }

        public TableBuilder AddHashPartitions(int buckets, params string[] columns)
        {
            return AddHashPartitions(buckets, 0, columns);
        }

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

        public TableBuilder AddRangePartition(
            Action<PartialRowOperation, PartialRowOperation> configure)
        {
            return AddRangePartition(
                configure,
                RangePartitionBound.Inclusive,
                RangePartitionBound.Exclusive);
        }

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

            var schema = new Schema(columns);
            var lowerBoundRow = new PartialRowOperation(schema, lowerRowOp);
            var upperBoundRow = new PartialRowOperation(schema, upperRowOp);

            configure(lowerBoundRow, upperBoundRow);

            _splitRowsRangeBounds.Add(lowerBoundRow);
            _splitRowsRangeBounds.Add(upperBoundRow);

            return this;
        }

        public TableBuilder AddSplitRow(Action<PartialRowOperation> configure)
        {
            // TODO: Rework this
            var columns = CreateTableRequest.Schema.Columns
                .Select(c => ColumnSchema.FromProtobuf(c))
                .ToList();

            var schema = new Schema(columns);
            var splitRow = new PartialRowOperation(schema, RowOperation.SplitRow);

            configure(splitRow);

            _splitRowsRangeBounds.Add(splitRow);

            return this;
        }

        public CreateTableRequestPB Build()
        {
            if (_splitRowsRangeBounds.Count > 0)
            {
                OperationsEncoder.ComputeSize(
                    _splitRowsRangeBounds,
                    out int rowSize,
                    out int indirectSize);

                var rowData = new byte[rowSize];
                var indirectData = new byte[indirectSize];

                OperationsEncoder.Encode(_splitRowsRangeBounds, rowData, indirectData);

                CreateTableRequest.SplitRowsRangeBounds.Rows = rowData;
                CreateTableRequest.SplitRowsRangeBounds.IndirectData = indirectData;
            }

            return CreateTableRequest;
        }

        public static implicit operator CreateTableRequestPB(TableBuilder builder) => builder.CreateTableRequest;
    }
}
