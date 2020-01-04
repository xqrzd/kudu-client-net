﻿using System;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Master;

namespace Knet.Kudu.Client.Builder
{
    public class TableBuilder
    {
        private readonly List<PartialRowOperation> _ranges;

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

            _ranges = new List<PartialRowOperation>();
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

        public TableBuilder AddRangePartition(Action<PartialRowOperation, PartialRowOperation> action)
        {
            // TODO: Rework this
            var columns = CreateTableRequest.Schema.Columns
                .Select(c => ColumnSchema.FromProtobuf(c))
                .ToList();

            var schema = new Schema(columns);
            var lowerBoundRow = new PartialRowOperation(schema, RowOperation.RangeLowerBound);
            var upperBoundRow = new PartialRowOperation(schema, RowOperation.RangeUpperBound);

            action(lowerBoundRow, upperBoundRow);

            _ranges.Add(lowerBoundRow);
            _ranges.Add(upperBoundRow);

            return this;
        }

        public CreateTableRequestPB Build()
        {
            if (_ranges.Count > 0)
            {
                OperationsEncoder.ComputeSize(
                    _ranges,
                    out int rowSize,
                    out int indirectSize);

                var rowData = new byte[rowSize];
                var indirectData = new byte[indirectSize];

                OperationsEncoder.Encode(_ranges, rowData, indirectData);

                CreateTableRequest.SplitRowsRangeBounds.Rows = rowData;
                CreateTableRequest.SplitRowsRangeBounds.IndirectData = indirectData;
            }

            return CreateTableRequest;
        }

        public static implicit operator CreateTableRequestPB(TableBuilder builder) => builder.CreateTableRequest;
    }
}
