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

        /// <summary>
        /// This table's owner or an empty string if the table was created
        /// without owner on a version of Kudu that didn't automatically
        /// assign an owner.
        /// </summary>
        public string Owner => SchemaPb.Owner;

        public override string ToString() => TableName;

        /// <summary>
        /// Get a new insert configured with this table's schema.
        /// </summary>
        public KuduOperation NewInsert() => NewOperation(RowOperation.Insert);

        /// <summary>
        /// Get a new insert ignore configured with this table's schema.
        /// An insert ignore will ignore duplicate row errors. This is useful
        /// when the same insert may be sent multiple times.
        /// </summary>
        public KuduOperation NewInsertIgnore() => NewOperation(RowOperation.InsertIgnore);

        /// <summary>
        /// Get a new update configured with this table's schema.
        /// </summary>
        public KuduOperation NewUpdate() => NewOperation(RowOperation.Update);

        /// <summary>
        /// Get a new upsert configured with this table's schema.
        /// </summary>
        public KuduOperation NewUpsert() => NewOperation(RowOperation.Upsert);

        /// <summary>
        /// Get a new delete configured with this table's schema.
        /// </summary>
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
