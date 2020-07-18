using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Protocol.Rpc;
using ProtoBuf;

namespace Knet.Kudu.Client.Util
{
    public static class ProtobufHelper
    {
        public static RowOperationsPB EncodeRowOperations(params PartialRowOperation[] rows)
        {
            return EncodeRowOperations(rows.ToList());
        }

        public static RowOperationsPB EncodeRowOperations(List<PartialRowOperation> rows)
        {
            OperationsEncoder.ComputeSize(
                rows,
                out int rowSize,
                out int indirectSize);

            var rowData = new byte[rowSize];
            var indirectData = new byte[indirectSize];

            OperationsEncoder.Encode(rows, rowData, indirectData);

            return new RowOperationsPB
            {
                Rows = rowData,
                IndirectData = indirectData
            };
        }

        public static ErrorStatusPB GetErrorStatus(ReadOnlySequence<byte> buffer)
        {
            return Serializer.Deserialize<ErrorStatusPB>(buffer);
        }

        public static ColumnTypeAttributes ToTypeAttributes(
            this ColumnTypeAttributesPB pb)
        {
            if (pb is null)
                return null;

            return new ColumnTypeAttributes(
                pb.ShouldSerializePrecision() ? pb.Precision : default,
                pb.ShouldSerializeScale() ? pb.Scale : default,
                pb.ShouldSerializeLength() ? pb.Length : default);
        }

        public static ColumnTypeAttributesPB ToTypeAttributesPb(
            this ColumnTypeAttributes attr)
        {
            if (attr is null)
                return null;

            var pb = new ColumnTypeAttributesPB();

            if (attr.Precision.HasValue)
                pb.Precision = attr.Precision.GetValueOrDefault();

            if (attr.Scale.HasValue)
                pb.Scale = attr.Scale.GetValueOrDefault();

            if (attr.Length.HasValue)
                pb.Length = attr.Length.GetValueOrDefault();

            return pb;
        }

        public static ColumnSchemaPB ToColumnSchemaPb(this ColumnSchema columnSchema)
        {
            var defaultValue = columnSchema.DefaultValue;
            var encodedDefaultValue = defaultValue != null
                ? KuduEncoder.EncodeDefaultValue(columnSchema, columnSchema.DefaultValue)
                : null;

            return new ColumnSchemaPB
            {
                Name = columnSchema.Name,
                Type = (DataTypePB)columnSchema.Type,
                IsKey = columnSchema.IsKey,
                IsNullable = columnSchema.IsNullable,
                ReadDefaultValue = encodedDefaultValue,
                CfileBlockSize = columnSchema.DesiredBlockSize,
                Encoding = (EncodingTypePB)columnSchema.Encoding,
                Compression = (CompressionTypePB)columnSchema.Compression,
                TypeAttributes = columnSchema.TypeAttributes.ToTypeAttributesPb(),
                Comment = columnSchema.Comment
            };
        }

        public static TabletServerInfo ToTabletServerInfo(
            this ListTabletServersResponsePB.Entry entry)
        {
            var tsUuid = entry.InstanceId.PermanentUuid.ToStringUtf8();
            var registration = entry.Registration;

            var rpcAddresses = new List<HostAndPort>(registration.RpcAddresses.Count);
            foreach (var rpcAddress in registration.RpcAddresses)
            {
                var hostPort = rpcAddress.ToHostAndPort();
                rpcAddresses.Add(hostPort);
            }

            var httpAddresses = new List<HostAndPort>(registration.HttpAddresses.Count);
            foreach (var rpcAddress in registration.HttpAddresses)
            {
                var hostPort = rpcAddress.ToHostAndPort();
                httpAddresses.Add(hostPort);
            }

            var startTime = DateTimeOffset.FromUnixTimeSeconds(registration.StartTime);

            return new TabletServerInfo(
                tsUuid,
                entry.MillisSinceHeartbeat,
                entry.Location,
                (TabletServerState)entry.State,
                rpcAddresses,
                httpAddresses,
                registration.SoftwareVersion,
                registration.HttpsEnabled,
                startTime.ToLocalTime());
        }

        public static PartitionSchema CreatePartitionSchema(
            PartitionSchemaPB partitionSchemaPb, KuduSchema schema)
        {
            var rangeSchema = new RangeSchema(ToColumnIds(
                partitionSchemaPb.RangeSchema.Columns));

            var hashBucketSchemas = new List<HashBucketSchema>(
                partitionSchemaPb.HashBucketSchemas.Count);

            foreach (var hashSchema in partitionSchemaPb.HashBucketSchemas)
            {
                var newSchema = new HashBucketSchema(
                    ToColumnIds(hashSchema.Columns),
                    hashSchema.NumBuckets,
                    hashSchema.Seed);

                hashBucketSchemas.Add(newSchema);
            }

            return new PartitionSchema(rangeSchema, hashBucketSchemas, schema);
        }

        private static List<int> ToColumnIds(
            List<PartitionSchemaPB.ColumnIdentifierPB> columns)
        {
            var columnIds = new List<int>(columns.Count);

            foreach (var column in columns)
                columnIds.Add(column.Id);

            return columnIds;
        }
    }
}
