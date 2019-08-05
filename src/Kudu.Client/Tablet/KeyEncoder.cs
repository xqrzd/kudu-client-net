using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using Kudu.Client.Internal;
using Kudu.Client.Util;

namespace Kudu.Client.Tablet
{
    public static partial class KeyEncoder
    {
        public static void EncodePartitionKey(
            PartialRow row, PartitionSchema partitionSchema, BufferWriter writer)
        {
            foreach (var hashSchema in partitionSchema.HashBucketSchemas)
            {
                var bucket = GetHashBucket(row, hashSchema);
                var slice = writer.GetSpan(4);
                BinaryPrimitives.WriteInt32BigEndian(slice, bucket);
                writer.Advance(4);
            }

            var rangeColumns = partitionSchema.RangeSchema.ColumnIds;
            EncodeColumns(row, rangeColumns, writer);
        }

        public static int GetHashBucket(PartialRow row, HashBucketSchema hashSchema)
        {
            using (var writer = new BufferWriter(256))
            {
                EncodeColumns(row, hashSchema.ColumnIds, writer);
                var hash = Murmur2.Hash64(writer.Memory.Span, hashSchema.Seed);
                var bucket = hash % (uint)hashSchema.NumBuckets;
                return (int)bucket;
            }
        }

        /// <summary>
        /// Encodes the provided row into a range partition key.
        /// </summary>
        /// <param name="row">The row to encode.</param>
        /// <param name="rangeSchema">The range partition schema.</param>
        public static byte[] EncodeRangePartitionKey(
            PartialRow row, RangeSchema rangeSchema)
        {
            using var writer = new BufferWriter(256);
            EncodeColumns(row, rangeSchema.ColumnIds, writer);
            return writer.Memory.ToArray();
        }

        private static void EncodeColumns(
            PartialRow row, List<int> columnIds, BufferWriter writer)
        {
            for (int i = 0; i < columnIds.Count; i++)
            {
                bool isLast = i + 1 == columnIds.Count;
                var columnIndex = row.Schema.GetColumnIndex(columnIds[i]);
                EncodeColumn(row, columnIndex, isLast, writer);
            }
        }

        private static void EncodeColumn(
            PartialRow row, int columnIndex, bool isLast, BufferWriter writer)
        {
            var schema = row.Schema;
            var column = schema.GetColumn(columnIndex);
            var type = column.Type;

            if (type == KuduType.String || type == KuduType.Binary)
            {
                var data = row.GetVarLengthColumn(columnIndex);
                EncodeBinary(data, writer, isLast);
            }
            else
            {
                // TODO: Benchmark MemoryMarshal.TryRead
                var size = column.Size;
                var slice = writer.GetSpan(size);

                var data = row.GetRowAllocColumn(columnIndex, size);
                data.CopyTo(slice);

                // Row data is little endian, but key encoding is big endian.
                slice.Reverse();

                if (column.IsSigned)
                    slice.SwapMostSignificantBitBigEndian();

                writer.Advance(size);
            }
        }

        private static void EncodeBinary(
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
