using System.Collections.Generic;

namespace Knet.Kudu.Client
{
    public class PartitionSchema
    {
        public RangeSchema RangeSchema { get; }

        public List<HashBucketSchema> HashBucketSchemas { get; }

        /// <summary>
        /// Returns true if the partition schema does not include any hash components,
        /// and the range columns match the table's primary key columns.
        /// </summary>
        public bool IsSimpleRangePartitioning { get; }

        public PartitionSchema(
            RangeSchema rangeSchema,
            List<HashBucketSchema> hashBucketSchemas,
            KuduSchema schema)
        {
            RangeSchema = rangeSchema;
            HashBucketSchemas = hashBucketSchemas;

            bool isSimple = hashBucketSchemas.Count == 0 &&
                rangeSchema.ColumnIds.Count == schema.PrimaryKeyColumnCount;

            if (isSimple)
            {
                int i = 0;
                foreach (int id in rangeSchema.ColumnIds)
                {
                    if (schema.GetColumnIndex(id) != i++)
                    {
                        isSimple = false;
                        break;
                    }
                }
            }

            IsSimpleRangePartitioning = isSimple;
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
