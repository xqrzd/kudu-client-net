using System;
using System.Collections.Generic;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client
{
    /// <summary>
    /// <para>
    /// A <see cref="KuduPartitioner"/> allows clients to determine the target
    /// partition of a row without actually performing a write. The set of
    /// partitions is eagerly fetched when the KuduPartitioner is constructed
    /// so that the actual partitioning step can be performed synchronously
    /// without any network trips.
    /// </para>
    /// 
    /// <para>
    /// Note: Because this operates on a metadata snapshot retrieved at
    /// construction time, it will not reflect any metadata changes to the
    /// table that have occurred since its creation.
    /// </para>
    /// </summary>
    public class KuduPartitioner
    {
        private readonly PartitionSchema _partitionSchema;
        private readonly List<RemoteTablet> _tablets;

        public KuduPartitioner(KuduTable table, List<RemoteTablet> tablets)
        {
            _partitionSchema = table.PartitionSchema;
            _tablets = tablets;
        }

        /// <summary>
        /// The number of partitions known by this partitioner.
        /// </summary>
        public int NumPartitions => _tablets.Count;

        /// <summary>
        /// Determine if the given row falls into a valid partition.
        /// </summary>
        /// <param name="row">The row to check.</param>
        public bool IsCovered(PartialRow row)
        {
            var result = GetResult(row);
            return result.IsCoveredRange;
        }

        /// <summary>
        /// Determine the partition index that the given row falls into.
        /// </summary>
        /// <param name="row">The row to be partitioned.</param>
        /// <returns>
        /// The resulting partition index.
        /// The result will be less than <see cref="NumPartitions"/>.
        /// </returns>
        public int PartitionRow(PartialRow row)
        {
            var result = GetResult(row);

            if (result.IsNonCoveredRange)
            {
                throw new NonCoveredRangeException(
                    result.NonCoveredRangeStart,
                    result.NonCoveredRangeEnd);
            }

            return result.Index;
        }

        private FindTabletResult GetResult(PartialRow row)
        {
            var partitionSchema = _partitionSchema;
            int maxSize = KeyEncoder.CalculateMaxPartitionKeySize(row, partitionSchema);
            Span<byte> buffer = stackalloc byte[maxSize];

            KeyEncoder.EncodePartitionKey(
                row,
                partitionSchema,
                buffer,
                out int bytesWritten);

            var partitionKey = buffer.Slice(0, bytesWritten);

            return RemoteTabletExtensions.FindTablet(_tablets, partitionKey);
        }
    }
}
