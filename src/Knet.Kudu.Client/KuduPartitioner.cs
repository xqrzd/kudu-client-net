using System;
using System.Collections.Generic;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;

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
    /// NOTE: Because this operates on a metadata snapshot retrieved at
    /// construction time, it will not reflect any metadata changes to the
    /// table that have occurred since its creation.
    /// </para>
    /// </summary>
    public class KuduPartitioner
    {
        private readonly PartitionSchema _partitionSchema;
        private readonly AvlTree _cache;

        public int NumPartitions { get; }

        public KuduPartitioner(KuduTable table, List<RemoteTablet> tablets)
        {
            _partitionSchema = table.PartitionSchema;
            _cache = new AvlTree();

            NumPartitions = tablets.Count;
            InitializeCache(tablets);
        }

        /// <summary>
        /// Determine if the given row falls into a valid partition.
        /// </summary>
        /// <param name="row">The row to check.</param>
        public bool IsCovered(PartialRow row)
        {
            var entry = GetCacheEntry(row);
            return entry.IsCoveredRange;
        }

        public PartitionResult GetRowTablet(PartialRow row)
        {
            var entry = GetCacheEntry(row);

            if (entry.IsNonCoveredRange)
            {
                throw new NonCoveredRangeException(
                    entry.LowerBoundPartitionKey,
                    entry.UpperBoundPartitionKey);
            }

            return new PartitionResult(entry.Tablet, entry.PartitionIndex);
        }

        private PartitionerLocationEntry GetCacheEntry(PartialRow row)
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

            return (PartitionerLocationEntry)_cache.FloorEntry(partitionKey);
        }

        private void InitializeCache(List<RemoteTablet> tablets)
        {
            var newEntries = new List<PartitionerLocationEntry>();
            int partitionIndex = 0;

            if (tablets.Count == 0)
            {
                // If there are no tablets in the response, then the table is empty. If
                // there were any tablets in the table they would have been returned, since
                // the master guarantees that if the partition key falls in a non-covered
                // range, the previous tablet will be returned, and we did not set an upper
                // bound partition key on the request.

                newEntries.Add(PartitionerLocationEntry.NewNonCoveredRange2(
                    Array.Empty<byte>(),
                    Array.Empty<byte>()));
            }
            else
            {
                // The comments below will reference the following diagram:
                //
                //   +---+   +---+---+
                //   |   |   |   |   |
                // A | B | C | D | E | F
                //   |   |   |   |   |
                //   +---+   +---+---+
                //
                // It depicts a tablet locations response from the master containing three
                // tablets: B, D and E. Three non-covered ranges are present: A, C, and F.
                // An RPC response containing B, D and E could occur if the lookup partition
                // key falls in A, B, or C, although the existence of A as an initial
                // non-covered range can only be inferred if the lookup partition key falls
                // in A.

                byte[] firstLowerBound = tablets[0].Partition.PartitionKeyStart;

                if (firstLowerBound.Length > 0)
                {
                    // There is an initial non-covered range, such as A.
                    newEntries.Add(PartitionerLocationEntry.NewNonCoveredRange2(
                        Array.Empty<byte>(), firstLowerBound));
                }

                // lastUpperBound tracks the upper bound of the previously processed
                // entry, so that we can determine when we have found a non-covered range.
                byte[] lastUpperBound = firstLowerBound;

                foreach (var tablet in tablets)
                {
                    byte[] tabletLowerBound = tablet.Partition.PartitionKeyStart;
                    byte[] tabletUpperBound = tablet.Partition.PartitionKeyEnd;

                    if (lastUpperBound.SequenceCompareTo(tabletLowerBound) < 0)
                    {
                        // There is a non-covered range between the previous tablet and this tablet.
                        // This will discover C while processing the tablet location for D.
                        newEntries.Add(PartitionerLocationEntry.NewNonCoveredRange2(
                            lastUpperBound, tabletLowerBound));
                    }

                    lastUpperBound = tabletUpperBound;

                    // Now add the tablet itself (such as B, D, or E).
                    newEntries.Add(PartitionerLocationEntry.NewTablet2(tablet, partitionIndex++));
                }

                if (lastUpperBound.Length > 0)
                {
                    // There is a non-covered range between the last tablet and the end
                    // of the partition key space, such as F.
                    newEntries.Add(PartitionerLocationEntry.NewNonCoveredRange2(
                        lastUpperBound, Array.Empty<byte>()));
                }
            }

            foreach (var entry in newEntries)
                _cache.Insert(entry);
        }

        private class PartitionerLocationEntry : TableLocationEntry
        {
            public int PartitionIndex { get; }

            public PartitionerLocationEntry(
                RemoteTablet tablet,
                byte[] lowerBoundPartitionKey,
                byte[] upperBoundPartitionKey,
                int partitionIndex) : base(tablet, lowerBoundPartitionKey, upperBoundPartitionKey, -1)
            {
                PartitionIndex = partitionIndex;
            }

            public static PartitionerLocationEntry NewNonCoveredRange2(
                byte[] lowerBoundPartitionKey,
                byte[] upperBoundPartitionKey)
            {
                return new PartitionerLocationEntry(
                    null,
                    lowerBoundPartitionKey,
                    upperBoundPartitionKey,
                    -1);
            }

            public static PartitionerLocationEntry NewTablet2(
                RemoteTablet tablet, int partitionIndex)
            {
                return new PartitionerLocationEntry(tablet, null, null, partitionIndex);
            }
        }
    }

    public readonly struct PartitionResult
    {
        public RemoteTablet Tablet { get; }

        public int PartitionIndex { get; }

        public PartitionResult(RemoteTablet tablet, int partitionIndex)
        {
            Tablet = tablet;
            PartitionIndex = partitionIndex;
        }
    }
}
