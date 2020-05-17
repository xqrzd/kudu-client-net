using System.Collections.Generic;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public class KuduTable
    {
        internal GetTableSchemaResponsePB SchemaPb { get; }

        internal GetTableSchemaResponsePB SchemaPbNoIds { get; }

        public KuduSchema Schema { get; }

        public PartitionSchema PartitionSchema { get; }

        public string TableId { get; }

        public IReadOnlyDictionary<string, string> ExtraConfig { get; }

        public KuduTable(GetTableSchemaResponsePB schemaPb)
        {
            Schema = new KuduSchema(schemaPb.Schema);
            SchemaPbNoIds = CreateWithNoColumnIds(schemaPb);
            SchemaPb = schemaPb;
            PartitionSchema = ProtobufHelper.CreatePartitionSchema(
                schemaPb.PartitionSchema, Schema);
            TableId = schemaPb.TableId.ToStringUtf8();
            ExtraConfig = schemaPb.ExtraConfigs;
        }

        public int NumReplicas => SchemaPb.NumReplicas;

        public string TableName => SchemaPb.TableName;

        public override string ToString() => TableName;

        public KuduOperation NewInsert() => NewOperation(RowOperation.Insert);

        public KuduOperation NewUpdate() => NewOperation(RowOperation.Update);

        public KuduOperation NewUpsert() => NewOperation(RowOperation.Upsert);

        public KuduOperation NewDelete() => NewOperation(RowOperation.Delete);

        private KuduOperation NewOperation(RowOperation rowOperation)
        {
            return new KuduOperation(this, rowOperation);
        }

        private static GetTableSchemaResponsePB CreateWithNoColumnIds(
            GetTableSchemaResponsePB schemaPb)
        {
            var clone = schemaPb.Clone();

            foreach (var column in clone.Schema.Columns)
            {
                column.ResetComment();
                column.ResetId();
            }

            return clone;
        }
    }
}

namespace Knet.Kudu.Client.Protocol.Master
{
    public partial class GetTableSchemaResponsePB
    {
        public GetTableSchemaResponsePB Clone()
        {
            var clone = (GetTableSchemaResponsePB)MemberwiseClone();
            var newSchema = new SchemaPB();
            var numColumns = clone.Schema.Columns.Count;

            for (int i = 0; i < numColumns; i++)
            {
                var column = clone.Schema.Columns[i].Clone();
                newSchema.Columns.Add(column);
            }

            clone.Schema = newSchema;

            return clone;
        }
    }
}

namespace Knet.Kudu.Client.Protocol
{
    public partial class ColumnSchemaPB
    {
        public ColumnSchemaPB Clone()
        {
            return (ColumnSchemaPB)MemberwiseClone();
        }
    }
}
