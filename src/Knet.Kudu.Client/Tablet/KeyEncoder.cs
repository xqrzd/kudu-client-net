using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Tablet
{
    public static partial class KeyEncoder
    {
        /// <summary>
        /// Encodes the primary key of the row.
        /// </summary>
        /// <param name="row">The row to encode.</param>
        public static byte[] EncodePrimaryKey(PartialRow row)
        {
            var schema = row.Schema;
            int primaryKeyColumnCount = schema.PrimaryKeyColumnCount;
            int maxSize = CalculateMaxPrimaryKeySize(row);
            Span<byte> buffer = stackalloc byte[maxSize];
            var slice = buffer;
            int bytesWritten = 0;

            for (int columnIdx = 0; columnIdx < primaryKeyColumnCount; columnIdx++)
            {
                bool isLast = columnIdx + 1 == primaryKeyColumnCount;
                EncodeColumn(row, columnIdx, isLast, slice, out int localBytesWritten);
                slice = slice.Slice(localBytesWritten);
                bytesWritten += localBytesWritten;
            }

            return buffer.Slice(0, bytesWritten).ToArray();
        }

        public static void EncodePartitionKey(
            PartialRow row,
            PartitionSchema partitionSchema,
            Span<byte> destination,
            out int bytesWritten)
        {
            int localBytesWritten = 0;

            foreach (var hashSchema in partitionSchema.HashBucketSchemas)
            {
                var bucket = GetHashBucket(row, hashSchema, destination.Length);
                var slice = destination.Slice(0, 4);
                BinaryPrimitives.WriteInt32BigEndian(slice, bucket);
                destination = destination.Slice(4);
                localBytesWritten += 4;
            }

            var rangeColumns = partitionSchema.RangeSchema.ColumnIds;
            EncodeColumns(row, rangeColumns, destination, out int written);

            bytesWritten = localBytesWritten + written;
        }

        public static int GetHashBucket(PartialRow row, HashBucketSchema hashSchema, int maxSize)
        {
            Span<byte> buffer = stackalloc byte[maxSize];
            EncodeColumns(row, hashSchema.ColumnIds, buffer, out int bytesWritten);
            var slice = buffer.Slice(0, bytesWritten);
            var hash = Murmur2.Hash64(slice, hashSchema.Seed);
            var bucket = hash % (uint)hashSchema.NumBuckets;
            return (int)bucket;
        }

        /// <summary>
        /// Encodes a hash bucket into the buffer.
        /// </summary>
        /// <param name="bucket">The bucket.</param>
        /// <param name="bufferWriter">The buffer.</param>
        public static void EncodeHashBucket(int bucket, IBufferWriter<byte> bufferWriter)
        {
            Span<byte> span = bufferWriter.GetSpan(4);
            BinaryPrimitives.WriteInt32BigEndian(span, bucket);
            bufferWriter.Advance(4);
        }

        /// <summary>
        /// Encodes the provided row into a range partition key.
        /// </summary>
        /// <param name="row">The row to encode.</param>
        /// <param name="rangeSchema">The range partition schema.</param>
        public static byte[] EncodeRangePartitionKey(
            PartialRow row, RangeSchema rangeSchema)
        {
            int maxSize = CalculateMaxPrimaryKeySize(row);
            Span<byte> buffer = stackalloc byte[maxSize];
            EncodeColumns(row, rangeSchema.ColumnIds, buffer, out int bytesWritten);
            return buffer.Slice(0, bytesWritten).ToArray();
        }

        private static void EncodeColumns(
            PartialRow row, List<int> columnIds, Span<byte> destination, out int bytesWritten)
        {
            var numColumns = columnIds.Count;
            var written = 0;

            for (int i = 0; i < numColumns; i++)
            {
                bool isLast = i + 1 == numColumns;
                var columnIndex = row.Schema.GetColumnIndex(columnIds[i]);
                EncodeColumn(row, columnIndex, isLast, destination, out int localBytesWritten);
                destination = destination.Slice(localBytesWritten);
                written += localBytesWritten;
            }

            bytesWritten = written;
        }

        private static void EncodeColumn(
            PartialRow row,
            int columnIndex,
            bool isLast,
            Span<byte> destination,
            out int bytesWritten)
        {
            var schema = row.Schema;
            var column = schema.GetColumn(columnIndex);

            if (column.IsFixedSize)
            {
                var size = column.Size;
                var slice = destination.Slice(0, size);

                var data = row.GetRowAllocColumn(columnIndex, size);
                data.CopyTo(slice);

                // Row data is little endian, but key encoding is big endian.
                slice.Reverse();

                if (column.IsSigned)
                    slice.SwapMostSignificantBitBigEndian();

                bytesWritten = size;
            }
            else
            {
                var data = row.GetVarLengthColumn(columnIndex);
                EncodeBinary(data, destination, isLast, out bytesWritten);
            }
        }

        private static void EncodeBinary(
            ReadOnlySpan<byte> source,
            Span<byte> destination,
            bool isLast,
            out int bytesWritten)
        {
            if (isLast)
            {
                source.CopyTo(destination);
                bytesWritten = source.Length;
            }
            else
            {
                int bytesWrittenx = EncodeBinary(source, destination);

                // TODO: If we have a guarantee this memory is
                // zeroed, this can be omitted.
                destination[bytesWrittenx++] = 0x0;
                destination[bytesWrittenx++] = 0x0;

                bytesWritten = bytesWrittenx;
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

        public static int CalculateMaxPrimaryKeySize(PartialRow row)
        {
            var schema = row.Schema;
            var primaryKeyColumnCount = schema.PrimaryKeyColumnCount;
            var size = 0;

            for (int i = 0; i < primaryKeyColumnCount; i++)
            {
                var column = schema.GetColumn(i);
                int length;

                if (column.IsFixedSize)
                {
                    length = column.Size;
                }
                else
                {
                    var isLast = i + 1 == primaryKeyColumnCount;
                    var data = row.GetVarLengthColumn(i);

                    if (isLast)
                        length = data.Length;
                    else
                        length = data.Length * 2 + 2;
                }

                size += length;
            }

            return size;
        }

        public static int CalculateMaxPartitionKeySize(
            PartialRow row, PartitionSchema partitionSchema)
        {
            var hashSchemaSize = partitionSchema.HashBucketSchemas.Count * 4;
            return CalculateMaxPrimaryKeySize(row) + hashSchemaSize;
        }
    }
}
