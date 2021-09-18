using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Client;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Internal;

internal static class ProtobufHelper
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
            Rows = UnsafeByteOperations.UnsafeWrap(rowData),
            IndirectData = UnsafeByteOperations.UnsafeWrap(indirectData)
        };
    }

    public static T[] ToArray<T>(this RepeatedField<T> source)
    {
        var length = source.Count;
        var arr = new T[length];

        for (int i = 0; i < length; i++)
        {
            arr[i] = source[i];
        }

        return arr;
    }

    public static ReadOnlyMemory<byte>[] ToMemoryArray(
        this RepeatedField<ByteString> source)
    {
        var length = source.Count;
        var results = new ReadOnlyMemory<byte>[length];

        for (int i = 0; i < length; i++)
        {
            var value = source[i];
            results[i] = value.Memory;
        }

        return results;
    }

    public static byte[] ToByteArray(IMessage message)
    {
        var size = message.CalculateSize();
        var buffer = new byte[size];
        message.WriteTo(buffer);

        return buffer;
    }

    public static ColumnTypeAttributes ToTypeAttributes(
        this ColumnTypeAttributesPB pb)
    {
        if (pb is null)
            return null;

        return new ColumnTypeAttributes(
            pb.HasPrecision ? pb.Precision : default,
            pb.HasScale ? pb.Scale : default,
            pb.HasLength ? pb.Length : default);
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
        var result = new ColumnSchemaPB
        {
            Name = columnSchema.Name,
            Type = (DataTypePB)columnSchema.Type,
            IsKey = columnSchema.IsKey,
            IsNullable = columnSchema.IsNullable,
            CfileBlockSize = columnSchema.DesiredBlockSize,
            Encoding = (EncodingTypePB)columnSchema.Encoding,
            Compression = (CompressionTypePB)columnSchema.Compression,
            TypeAttributes = columnSchema.TypeAttributes.ToTypeAttributesPb()
        };

        CopyDefaultValueToPb(columnSchema, result);

        if (columnSchema.Comment is not null)
        {
            result.Comment = columnSchema.Comment;
        }

        return result;
    }

    public static void CopyDefaultValueToPb(ColumnSchema columnSchema, ColumnSchemaPB columnSchemaPb)
    {
        var defaultValue = columnSchema.DefaultValue;
        if (defaultValue is not null)
        {
            var encodedDefaultValue = KuduEncoder.EncodeDefaultValue(columnSchema, defaultValue);
            columnSchemaPb.ReadDefaultValue = UnsafeByteOperations.UnsafeWrap(encodedDefaultValue);
        }
    }

    public static Partition ToPartition(PartitionPB partition)
    {
        return new Partition(
            partition.PartitionKeyStart.ToByteArray(),
            partition.PartitionKeyEnd.ToByteArray(),
            partition.HashBuckets.ToArray());
    }

    public static PartitionPB ToPartitionPb(Partition partition)
    {
        var partitionPb = new PartitionPB
        {
            PartitionKeyStart = UnsafeByteOperations.UnsafeWrap(partition.PartitionKeyStart),
            PartitionKeyEnd = UnsafeByteOperations.UnsafeWrap(partition.PartitionKeyEnd)
        };

        partitionPb.HashBuckets.AddRange(partition.HashBuckets);

        return partitionPb;
    }

    public static HiveMetastoreConfig ToHiveMetastoreConfig(
        this Protobuf.Master.HiveMetastoreConfig hmsConfig)
    {
        if (hmsConfig is null)
            return null;

        return new HiveMetastoreConfig(
            hmsConfig.HmsUris,
            hmsConfig.HmsSaslEnabled,
            hmsConfig.HmsUuid);
    }

    public static HostPortPB ToHostPortPb(HostAndPort hostAndPort)
    {
        return new HostPortPB
        {
            Host = hostAndPort.Host,
            Port = (uint)hostAndPort.Port
        };
    }

    public static ServerMetadataPB ToServerMetadataPb(ServerInfo serverInfo)
    {
        var serverMetadataPb = new ServerMetadataPB
        {
            Uuid = ByteString.CopyFromUtf8(serverInfo.Uuid)
        };

        if (serverInfo.HasLocation)
        {
            serverMetadataPb.Location = serverInfo.Location;
        }

        var hostPortPb = ToHostPortPb(serverInfo.HostPort);
        serverMetadataPb.RpcAddresses.Add(hostPortPb);

        return serverMetadataPb;
    }

    public static TabletServerInfo ToTabletServerInfo(
        this ListTabletServersResponsePB.Types.Entry entry)
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
        RepeatedField<PartitionSchemaPB.Types.ColumnIdentifierPB> columns)
    {
        var columnIds = new List<int>(columns.Count);

        foreach (var column in columns)
            columnIds.Add(column.Id);

        return columnIds;
    }

    public static int WriteRawVarint32(Span<byte> buffer, uint value)
    {
        // Optimize for the common case of a single byte value.
        if (value < 128)
        {
            buffer[0] = (byte)value;
            return 1;
        }

        var position = 0;

        while (value > 127)
        {
            buffer[position++] = (byte)(value & 0x7F | 0x80);
            value >>= 7;
        }

        buffer[position++] = (byte)value;
        return position;
    }
}
