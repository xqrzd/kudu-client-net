using System;
using Kudu.Client.Protocol;
using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Builder
{
    public class TableBuilder
    {
        internal CreateTableRequestPB CreateTableRequest;

        public TableBuilder()
        {
            CreateTableRequest = new CreateTableRequestPB
            {
                Schema = new SchemaPB(),
                PartitionSchema = new PartitionSchemaPB()
            };
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
                partition.Columns.Add(new PartitionSchemaPB.ColumnIdentifierPB
                {
                    Name = column
                });
            }

            CreateTableRequest.PartitionSchema.HashBucketSchemas.Add(partition);

            return this;
        }

        public static implicit operator CreateTableRequestPB(TableBuilder builder) => builder.CreateTableRequest;
    }
}
