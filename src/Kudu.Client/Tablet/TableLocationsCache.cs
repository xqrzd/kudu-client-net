using System;
using System.Collections.Generic;
using System.Threading;
using Kudu.Client.Internal;

namespace Kudu.Client.Tablet
{
    /// <summary>
    /// A cache of the tablet locations in a table, keyed by partition key. Unlike the
    /// Java client, we don't store non-covered ranges. If FindTablet returns null, it
    /// could mean the tablet doesn't exist, or hasn't been cached yet. A master must
    /// be contacted to make a determination.
    /// </summary>
    public class TableLocationsCache : IDisposable
    {
        private readonly AvlTree _cache;
        private readonly ReaderWriterLockSlim _lock;

        public TableLocationsCache()
        {
            _cache = new AvlTree();
            _lock = new ReaderWriterLockSlim();
        }

        /// <summary>
        /// Retrieves the tablet that the given partition key should go to, or null if
        /// a tablet couldn't be found.
        /// </summary>
        /// <param name="partitionKey">The partition key to look up.</param>
        public RemoteTablet FindTablet(ReadOnlySpan<byte> partitionKey)
        {
            RemoteTablet tablet;
            _lock.EnterReadLock();
            try
            {
                tablet = _cache.GetFloor(partitionKey);
            }
            finally
            {
                _lock.ExitReadLock();
            }

            if (tablet != null && (!tablet.Partition.IsEndPartition &&
                partitionKey.SequenceCompareTo(tablet.Partition.PartitionKeyEnd) >= 0))
            {
                // The requested partition key is outside the bounds of this tablet.
                tablet = null;
            }

            return tablet;
        }

        public void CacheTabletLocations(
            List<RemoteTablet> tablets,
            ReadOnlySpan<byte> requestPartitionKey)
        {
            if (tablets.Count == 0)
            {
                // If there are no tablets in the response, then the table is empty. If
                // there were any tablets in the table they would have been returned, since
                // the master guarantees that if the partition key falls in a non-covered
                // range, the previous tablet will be returned, and we did not set an upper
                // bound partition key on the request.
                ClearCache();
            }
            else
            {
                byte[] discoveredlowerBound = tablets[0].Partition.PartitionKeyStart;
                byte[] discoveredUpperBound = tablets[tablets.Count - 1].Partition.PartitionKeyEnd;

                _lock.EnterWriteLock();
                try
                {
                    // Remove all existing overlapping entries, and add the new entries.
                    RemoteTablet floorEntry = _cache.GetFloor(discoveredlowerBound);

                    if (floorEntry != null && requestPartitionKey.SequenceCompareTo(
                        floorEntry.Partition.PartitionKeyEnd) < 0)
                    {
                        // A new partition now covers part of an old partition.
                        discoveredlowerBound = floorEntry.Partition.PartitionKeyStart;
                    }

                    bool upperBoundActive = discoveredUpperBound.Length > 0;
                    _cache.ClearRange(discoveredlowerBound, discoveredUpperBound, upperBoundActive);

                    foreach (var entry in tablets)
                        _cache.Insert(entry);
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }
        }

        public void ClearCache()
        {
            _lock.EnterWriteLock();
            try
            {
                _cache.Clear();
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public void Dispose()
        {
            _lock.Dispose();
        }
    }
}
