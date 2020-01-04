using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public class KuduTable
    {
        internal GetTableSchemaResponsePB SchemaPb { get; }

        public Schema Schema { get; }

        public PartitionSchema PartitionSchema { get; }

        public string TableId { get; }

        public KuduTable(GetTableSchemaResponsePB schemaPb)
        {
            Schema = new Schema(schemaPb.Schema);
            SchemaPb = schemaPb;
            PartitionSchema = new PartitionSchema(schemaPb.PartitionSchema);
            TableId = schemaPb.TableId.ToStringUtf8();
        }

        public int NumReplicas => SchemaPb.NumReplicas;

        public string TableName => SchemaPb.TableName;

        public KuduOperation NewInsert() => NewOperation(RowOperation.Insert);

        public KuduOperation NewUpdate() => NewOperation(RowOperation.Update);

        public KuduOperation NewUpsert() => NewOperation(RowOperation.Upsert);

        public KuduOperation NewDelete() => NewOperation(RowOperation.Delete);

        private KuduOperation NewOperation(RowOperation rowOperation)
        {
            return new KuduOperation(this, rowOperation);
        }
    }
}
