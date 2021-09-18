using System;
using System.Collections.Generic;
using System.Threading;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Tablet;

/// <summary>
/// A cache of the tablet locations in a table, keyed by partition key.
/// Entries in the cache are either tablets or non-covered ranges.
/// </summary>
public sealed class TableLocationsCache : IDisposable
{
    private readonly ISystemClock _systemClock;
    private readonly AvlTree _cache;
    private readonly ReaderWriterLockSlim _lock;

    public TableLocationsCache(ISystemClock systemClock)
    {
        _systemClock = systemClock;
        _cache = new AvlTree();
        _lock = new ReaderWriterLockSlim();
    }

    public void Dispose()
    {
        _lock.Dispose();
    }

    /// <summary>
    /// Retrieves the tablet that the given partition key should go to, or null if
    /// a tablet couldn't be found.
    /// </summary>
    /// <param name="partitionKey">The partition key to look up.</param>
    public TableLocationEntry GetEntry(ReadOnlySpan<byte> partitionKey)
    {
        TableLocationEntry entry = GetFloorEntry(partitionKey);

        if (entry != null)
        {
            byte[] upperBoundPartitionKey = entry.UpperBoundPartitionKey;

            if (upperBoundPartitionKey.Length > 0 &&
                partitionKey.SequenceCompareTo(upperBoundPartitionKey) >= 0)
            {
                // The requested partition key is outside the bounds of this tablet.
                return null;
            }

            long now = _systemClock.CurrentMilliseconds;

            if (now > entry.Expiration)
                return null;
        }

        return entry;
    }

    /// <summary>
    /// Add tablet locations to the cache. Already known tablet locations will
    /// have their entry updated and expiration extended.
    /// </summary>
    /// <param name="tablets">The discovered tablets to cache.</param>
    /// <param name="requestPartitionKey">The lookup partition key.</param>
    /// <param name="requestedBatchSize">
    /// The number of tablet locations requested from the master in the
    /// original request.
    /// </param>
    /// <param name="ttl">
    /// The time in milliseconds that the tablets may be cached for.
    /// </param>
    public void CacheTabletLocations(
        List<RemoteTablet> tablets,
        ReadOnlySpan<byte> requestPartitionKey,
        int requestedBatchSize,
        long ttl)
    {
        long expiration = _systemClock.CurrentMilliseconds + ttl;
        var newEntries = new List<TableLocationEntry>();

        if (tablets.Count == 0)
        {
            // If there are no tablets in the response, then the table is empty. If
            // there were any tablets in the table they would have been returned, since
            // the master guarantees that if the partition key falls in a non-covered
            // range, the previous tablet will be returned, and we did not set an upper
            // bound partition key on the request.

            newEntries.Add(TableLocationEntry.NewNonCoveredRange(
                Array.Empty<byte>(),
                Array.Empty<byte>(),
                expiration));
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

            if (requestPartitionKey.SequenceCompareTo(firstLowerBound) < 0)
            {
                // If the first tablet is past the requested partition key, then the
                // partition key falls in an initial non-covered range, such as A.
                newEntries.Add(TableLocationEntry.NewNonCoveredRange(
                    Array.Empty<byte>(), firstLowerBound, expiration));
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
                    newEntries.Add(TableLocationEntry.NewNonCoveredRange(
                        lastUpperBound, tabletLowerBound, expiration));
                }

                lastUpperBound = tabletUpperBound;

                // Now add the tablet itself (such as B, D, or E).
                newEntries.Add(TableLocationEntry.NewCoveredRange(tablet, expiration));
            }

            if (lastUpperBound.Length > 0 &&
                tablets.Count < requestedBatchSize)
            {
                // There is a non-covered range between the last tablet and the end
                // of the partition key space, such as F.
                newEntries.Add(TableLocationEntry.NewNonCoveredRange(
                    lastUpperBound, Array.Empty<byte>(), expiration));
            }
        }

        byte[] discoveredlowerBound = newEntries[0].LowerBoundPartitionKey;
        byte[] discoveredUpperBound = newEntries[newEntries.Count - 1].UpperBoundPartitionKey;

        _lock.EnterWriteLock();
        try
        {
            // Remove all existing overlapping entries, and add the new entries.
            TableLocationEntry floorEntry = _cache.FloorEntry(discoveredlowerBound);
            if (floorEntry != null &&
                requestPartitionKey.SequenceCompareTo(floorEntry.UpperBoundPartitionKey) < 0)
            {
                discoveredlowerBound = floorEntry.LowerBoundPartitionKey;
            }

            bool upperBoundActive = discoveredUpperBound.Length > 0;
            _cache.ClearRange(discoveredlowerBound, discoveredUpperBound, upperBoundActive);

            foreach (var entry in newEntries)
                _cache.Insert(entry);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void RemoveTablet(ReadOnlySpan<byte> partitionKeyStart)
    {
        _lock.EnterWriteLock();
        try
        {
            _cache.Delete(partitionKeyStart);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void UpdateTablet(RemoteTablet tablet)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_cache.Search(tablet.Partition.PartitionKeyStart, out var cacheEntry) &&
                cacheEntry.IsCoveredRange)
            {
                var newEntry = TableLocationEntry.NewCoveredRange(tablet, cacheEntry.Expiration);
                _cache.Insert(newEntry);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private TableLocationEntry GetFloorEntry(ReadOnlySpan<byte> partitionKey)
    {
        _lock.EnterReadLock();
        try
        {
            return _cache.FloorEntry(partitionKey);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }
}

public class TableLocationEntry
{
    /// <summary>
    /// The lower bound partition key.
    /// </summary>
    public byte[] LowerBoundPartitionKey { get; }

    /// <summary>
    /// The upper bound partition key.
    /// </summary>
    public byte[] UpperBoundPartitionKey { get; }

    /// <summary>
    /// The remote tablet, only set if this entry represents a tablet.
    /// </summary>
    public RemoteTablet Tablet { get; }

    /// <summary>
    /// When this entry will expire, based on <see cref="ISystemClock"/>.
    /// </summary>
    public long Expiration { get; }

    public TableLocationEntry(
        RemoteTablet tablet,
        byte[] lowerBoundPartitionKey,
        byte[] upperBoundPartitionKey,
        long expiration)
    {
        Tablet = tablet;
        LowerBoundPartitionKey = lowerBoundPartitionKey;
        UpperBoundPartitionKey = upperBoundPartitionKey;
        Expiration = expiration;
    }

    /// <summary>
    /// If this entry is a non-covered range.
    /// </summary>
    public bool IsNonCoveredRange => Tablet is null;

    /// <summary>
    /// If this entry is a covered range.
    /// </summary>
    public bool IsCoveredRange => Tablet is not null;

    public static TableLocationEntry NewNonCoveredRange(
        byte[] lowerBoundPartitionKey,
        byte[] upperBoundPartitionKey,
        long expiration)
    {
        return new TableLocationEntry(
            null,
            lowerBoundPartitionKey,
            upperBoundPartitionKey,
            expiration);
    }

    public static TableLocationEntry NewCoveredRange(RemoteTablet tablet, long expiration)
    {
        var partition = tablet.Partition;
        var lowerBoundPartitionKey = partition.PartitionKeyStart;
        var upperBoundPartitionKey = partition.PartitionKeyEnd;

        return new TableLocationEntry(
            tablet, lowerBoundPartitionKey, upperBoundPartitionKey, expiration);
    }
}
