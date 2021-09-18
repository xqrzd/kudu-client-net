using System.Collections.Generic;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf.Master;

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

        /// <summary>
        /// The comment on the table.
        /// </summary>
        public string Comment => SchemaPb.Comment;

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
        /// Get a new update ignore configured with this table's schema. An update
        /// ignore will ignore missing row errors. This is useful to update a row
        /// only if it exists.
        /// </summary>
        public KuduOperation NewUpdateIgnore() => NewOperation(RowOperation.UpdateIgnore);

        /// <summary>
        /// Get a new upsert configured with this table's schema.
        /// </summary>
        public KuduOperation NewUpsert() => NewOperation(RowOperation.Upsert);

        /// <summary>
        /// Get a new delete configured with this table's schema.
        /// </summary>
        public KuduOperation NewDelete() => NewOperation(RowOperation.Delete);

        /// <summary>
        /// Get a new delete ignore configured with this table's schema. An delete
        /// ignore will ignore missing row errors. This is useful to delete a row
        /// only if it exists.
        /// </summary>
        public KuduOperation NewDeleteIgnore() => NewOperation(RowOperation.DeleteIgnore);

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
                column.ClearComment();
                column.ClearId();
            }

            return clone;
        }
    }
}
