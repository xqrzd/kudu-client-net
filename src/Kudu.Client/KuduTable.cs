using Kudu.Client.Protocol.Master;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public class KuduTable
    {
        private readonly Schema _schema;

        // TODO: Create a managed class for this.
        public GetTableSchemaResponsePB SchemaPb { get; }

        public string TableId { get; }

        public KuduTable(GetTableSchemaResponsePB schemaPb)
        {
            _schema = new Schema(schemaPb.Schema);
            SchemaPb = schemaPb;
            TableId = schemaPb.TableId.ToStringUtf8();
        }

        public int NumReplicas => SchemaPb.NumReplicas;

        public string TableName => SchemaPb.TableName;

        public Operation NewInsert() => NewOperation(RowOperation.Insert);

        public Operation NewUpdate() => NewOperation(RowOperation.Update);

        public Operation NewUpsert() => NewOperation(RowOperation.Upsert);

        public Operation NewDelete() => NewOperation(RowOperation.Delete);

        private Operation NewOperation(RowOperation rowOperation)
        {
            var row = new PartialRow(_schema, rowOperation);
            return new Operation(this, row);
        }
    }
}
