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

        public Operation NewInsert() => NewOperation(ChangeType.Insert);

        public Operation NewUpdate() => NewOperation(ChangeType.Update);

        public Operation NewUpsert() => NewOperation(ChangeType.Upsert);

        public Operation NewDelete() => NewOperation(ChangeType.Delete);

        private Operation NewOperation(ChangeType changeType)
        {
            var row = new PartialRow(_schema, changeType);
            return new Operation(this, row);
        }
    }
}
