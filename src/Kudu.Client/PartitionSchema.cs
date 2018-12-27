using System.Collections.Generic;
using Kudu.Client.Protocol;

namespace Kudu.Client
{
    public class PartitionSchema
    {
        public RangeSchema RangeSchema { get; }

        public List<HashBucketSchema> HashBucketSchemas { get; }

        public PartitionSchema(RangeSchema rangeSchema, List<HashBucketSchema> hashBucketSchemas)
        {
            RangeSchema = rangeSchema;
            HashBucketSchemas = hashBucketSchemas;
            // TODO: Calculate IsSimpleRangePartitioning
        }

        public PartitionSchema(PartitionSchemaPB partitionSchemaPb)
        {
            RangeSchema = new RangeSchema(ToColumnIds(partitionSchemaPb.RangeSchema.Columns));

            HashBucketSchemas = new List<HashBucketSchema>(partitionSchemaPb.HashBucketSchemas.Count);
            foreach (var hashSchema in partitionSchemaPb.HashBucketSchemas)
            {
                var newSchema = new HashBucketSchema(
                    ToColumnIds(hashSchema.Columns),
                    hashSchema.NumBuckets,
                    hashSchema.Seed);

                HashBucketSchemas.Add(newSchema);
            }
        }

        private static List<int> ToColumnIds(
            List<PartitionSchemaPB.ColumnIdentifierPB> columns)
        {
            var columnIds = new List<int>(columns.Count);

            foreach (var column in columns)
                columnIds.Add(column.Id);

            return columnIds;
        }
    }

    public class RangeSchema
    {
        public List<int> ColumnIds { get; }

        public RangeSchema(List<int> columnIds)
        {
            ColumnIds = columnIds;
        }
    }

    public class HashBucketSchema
    {
        public List<int> ColumnIds { get; }

        public int NumBuckets { get; }

        public uint Seed { get; }

        public HashBucketSchema(List<int> columnIds, int numBuckets, uint seed)
        {
            ColumnIds = columnIds;
            NumBuckets = numBuckets;
            Seed = seed;
        }
    }
}
