using Kudu.Client.Protocol.Master;

namespace Kudu.Client
{
    public class KuduTable
    {
        private readonly Schema _schema;

        public KuduTable(GetTableSchemaResponsePB schemaPb)
        {
            _schema = new Schema(schemaPb.Schema);
            SchemaPb = schemaPb;
        }

        // TODO: Create a managed class for this.
        public GetTableSchemaResponsePB SchemaPb { get; }

        public PartialRow NewInsert()
        {
            return new PartialRow(_schema, Protocol.RowOperationsPB.Type.Insert);
        }

        public PartialRow NewUpdate()
        {
            return new PartialRow(_schema, Protocol.RowOperationsPB.Type.Update);
        }

        public PartialRow NewUpsert()
        {
            return new PartialRow(_schema, Protocol.RowOperationsPB.Type.Upsert);
        }

        public PartialRow NewDelete()
        {
            return new PartialRow(_schema, Protocol.RowOperationsPB.Type.Delete);
        }
    }
}
