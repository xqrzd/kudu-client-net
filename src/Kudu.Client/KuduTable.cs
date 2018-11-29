using Kudu.Client.Protocol.Master;

namespace Kudu.Client
{
    public class KuduTable
    {
        // TODO: Create a managed class for this.
        public GetTableSchemaResponsePB Schema { get; }
        //public Schema Schema { get; }
        private readonly Schema _schema;

        public KuduTable(GetTableSchemaResponsePB schema)
        {
            var s = new Schema(schema);
            Schema = s.TableSchema;
            _schema = s;
        }

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
