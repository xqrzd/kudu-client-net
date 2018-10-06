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
    public static class KeyEncoder
    {
        public static void EncodePartitionKey(
            PartialRow row,
            PartitionSchemaPB partitionSchema,
            // TODO: Change this to Span<byte>?
            RecyclableMemoryStream stream)
        {
            foreach (var hashSchema in partitionSchema.HashBucketSchemas)
            {
                var bucket = GetHashBucket(row, hashSchema);
                var slice = stream.GetSpan(4);
                BinaryPrimitives.WriteInt32BigEndian(slice, bucket);
            }

            var rangeColumns = partitionSchema.RangeSchema.Columns.Select(c => c.Id).ToList();
            EncodeColumns(row, rangeColumns, stream);
        }

        public static int GetHashBucket(PartialRow row, HashBucketSchemaPB hashSchema)
        {
            using (var ms = new RecyclableMemoryStream(256))
            {
                var columns = hashSchema.Columns.Select(c => c.Id).ToList();
                EncodeColumns(row, columns, ms);
                var hash = Murmur2.Hash64(ms.AsSpan(), hashSchema.Seed);
                var bucket = hash % (uint)hashSchema.NumBuckets;
                return (int)bucket;// TODO: Return type
            }
        }

        private static void EncodeColumns(PartialRow row, List<int> columnIds, RecyclableMemoryStream stream)
        {
            for (int i = 0; i < columnIds.Count; i++)
            {
                bool isLast = i + 1 == columnIds.Count;
                var columnIndex = row.Schema.GetColumnIndex(columnIds[i]);
                EncodeColumn(row, columnIndex, isLast, stream);
            }
        }

        private static void EncodeColumn(
            PartialRow row,
            int columnIndex,
            bool isLast,
            RecyclableMemoryStream stream)
        {
            var schema = row.Schema;
            var type = schema.GetColumnType(columnIndex);

            if (type == DataType.String || type == DataType.Binary)
            {
                throw new NotImplementedException();
            }
            else
            {
                // TODO: Benchmark MemoryMarshal.TryRead
                var size = schema.GetColumnSize(columnIndex);
                var slice = stream.GetSpan(size);

                var data = row.GetSpanInRowAlloc(columnIndex);
                data.Slice(0, size).CopyTo(slice);

                // Row data is little endian, but key encoding is big endian.
                slice.Reverse();


                if (Schema.IsSigned(type))
                    slice.SwapMostSignificantBitBigEndian();
            }
        }

        private static void EncodeBinary(
            ReadOnlySpan<byte> value, bool isLast, bool hasZeros, Span<byte> buffer)
        {
            if (isLast)
            {
                value.CopyTo(buffer);
            }
            else
            {
                if (hasZeros)
                {
                    // TODO: Handle the case where value contains zeros.
                    throw new NotImplementedException();
                }
                else
                {
                    value.CopyTo(buffer);

                    buffer[value.Length] = 0x0;
                    buffer[value.Length + 1] = 0x0;
                }
            }
        }
    }
}
