using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using Kudu.Client.Internal;
using Kudu.Client.Protocol;
using Kudu.Client.Util;
using static Kudu.Client.Protocol.PartitionSchemaPB;

namespace Kudu.Client
{
    public static partial class KeyEncoder
    {
        public static void EncodePartitionKey(
            PartialRow row, PartitionSchemaPB partitionSchema, BufferWriter writer)
        {
            foreach (var hashSchema in partitionSchema.HashBucketSchemas)
            {
                var bucket = GetHashBucket(row, hashSchema);
                var slice = writer.GetSpan(4);
                BinaryPrimitives.WriteInt32BigEndian(slice, bucket);
                writer.Advance(4);
            }

            var rangeColumns = partitionSchema.RangeSchema.Columns;
            EncodeColumns(row, rangeColumns, writer);
        }

        public static int GetHashBucket(PartialRow row, HashBucketSchemaPB hashSchema)
        {
            using (var writer = new BufferWriter(256))
            {
                EncodeColumns(row, hashSchema.Columns, writer);
                var hash = Murmur2.Hash64(writer.Memory.Span, hashSchema.Seed);
                var bucket = hash % (uint)hashSchema.NumBuckets;
                return (int)bucket;
            }
        }

        private static void EncodeColumns(
            PartialRow row, List<ColumnIdentifierPB> columns, BufferWriter writer)
        {
            for (int i = 0; i < columns.Count; i++)
            {
                bool isLast = i + 1 == columns.Count;
                var columnIndex = row.Schema.GetColumnIndex(columns[i].Id);
                EncodeColumn(row, columnIndex, isLast, writer);
            }
        }

        private static void EncodeColumn(
            PartialRow row, int columnIndex, bool isLast, BufferWriter writer)
        {
            var schema = row.Schema;
            var type = schema.GetColumnType(columnIndex);

            if (type == DataTypePB.String || type == DataTypePB.Binary)
            {
                var data = row.GetSpanInVarLenData(columnIndex);
                EncodeBinary(data, writer, isLast);
            }
            else
            {
                // TODO: Benchmark MemoryMarshal.TryRead
                var size = schema.GetColumnSize(columnIndex);
                var slice = writer.GetSpan(size);

                var data = row.GetSpanInRowAlloc(columnIndex);
                data.Slice(0, size).CopyTo(slice);

                // Row data is little endian, but key encoding is big endian.
                slice.Reverse();

                if (Schema.IsSigned(type))
                    slice.SwapMostSignificantBitBigEndian();

                writer.Advance(size);
            }
        }

        public static void EncodeBinary(
            ReadOnlySpan<byte> source, BufferWriter writer, bool isLast)
        {
            if (isLast)
            {
                var span = writer.GetSpan(source.Length);
                source.CopyTo(span);
                writer.Advance(source.Length);
            }
            else
            {
                // Make sure we have enough space for the worst case
                // where every byte is 0.
                var span = writer.GetSpan(source.Length * 2 + 2);

                int bytesWritten = EncodeBinary(source, span);

                span[bytesWritten++] = 0x0;
                span[bytesWritten++] = 0x0;

                writer.Advance(bytesWritten);
            }
        }

        private static int EncodeBinarySlow(
            ReadOnlySpan<byte> source, Span<byte> destination)
        {
            int length = 0;

            foreach (byte b in source)
            {
                if (b == 0)
                {
                    destination[length++] = 0;
                    destination[length++] = 1;
                }
                else
                {
                    destination[length++] = b;
                }
            }

            return length;
        }
    }
}
