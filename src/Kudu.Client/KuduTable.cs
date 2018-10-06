using Kudu.Client.Protocol.Master;

namespace Kudu.Client
{
    public class KuduTable
    {
        // TODO: Create a managed class for this.
        public GetTableSchemaResponsePB Schema { get; }

        public KuduTable(GetTableSchemaResponsePB schema)
        {
            Schema = schema;
        }

        public PartialRow NewInsert()
        {
            return new PartialRow(new Schema(Schema.Schema, Schema), Protocol.RowOperationsPB.Type.Insert);
        }

        public PartialRow NewUpdate()
        {
            return new PartialRow(new Schema(Schema.Schema, Schema), Protocol.RowOperationsPB.Type.Update);
        }

        public PartialRow NewUpsert()
        {
            return new PartialRow(new Schema(Schema.Schema, Schema), Protocol.RowOperationsPB.Type.Upsert);
        }

        public PartialRow NewDelete()
        {
            return new PartialRow(new Schema(Schema.Schema, Schema), Protocol.RowOperationsPB.Type.Upsert);
        }
    }
}
