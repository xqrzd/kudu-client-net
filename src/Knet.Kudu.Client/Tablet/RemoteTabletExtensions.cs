using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Knet.Kudu.Client.Tablet;

public static class RemoteTabletExtensions
{
    public static FindTabletResult FindTablet(
        this List<RemoteTablet> tablets, ReadOnlySpan<byte> partitionKey)
    {
        int lo = 0;
        int hi = tablets.Count - 1;

        // If length == 0, hi == -1, and loop will not be entered
        while (lo <= hi)
        {
            // PERF: `lo` or `hi` will never be negative inside the loop,
            //       so computing median using uints is safe since we know
            //       `length <= int.MaxValue`, and indices are >= 0
            //       and thus cannot overflow an uint.
            //       Saves one subtraction per loop compared to
            //       `int i = lo + ((hi - lo) >> 1);`
            int i = (int)(((uint)hi + (uint)lo) >> 1);

            var tablet = tablets[i];
            int c = partitionKey.SequenceCompareTo(tablet.Partition.PartitionKeyStart);
            if (c == 0)
            {
                return new FindTabletResult(tablet, i);
            }
            else if (c > 0)
            {
                lo = i + 1;
            }
            else
            {
                hi = i - 1;
            }
        }

        if (hi >= 0)
        {
            var tablet = tablets[hi];
            if (tablet.Partition.ContainsPartitionKey(partitionKey))
            {
                return new FindTabletResult(tablet, hi);
            }

            return HandleMissingTablet(tablets, lo, tablet);
        }

        // The key is before the first partition.
        return HandleMissingTablet(tablets);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static FindTabletResult HandleMissingTablet(List<RemoteTablet> tablets)
    {
        var nonCoveredRangeEnd = tablets.Count == 0
            ? Array.Empty<byte>()
            : tablets[0].Partition.PartitionKeyStart;

        return new FindTabletResult(Array.Empty<byte>(), nonCoveredRangeEnd);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static FindTabletResult HandleMissingTablet(
        List<RemoteTablet> tablets, int nextIndex, RemoteTablet tablet)
    {
        var nonCoveredRangeStart = tablet.Partition.PartitionKeyEnd;

        var nonCoveredRangeEnd = tablets.Count == nextIndex
            ? Array.Empty<byte>()
            : tablets[nextIndex].Partition.PartitionKeyStart;

        return new FindTabletResult(nonCoveredRangeStart, nonCoveredRangeEnd);
    }
}

public readonly struct FindTabletResult
{
    public RemoteTablet Tablet { get; }

    public int Index { get; }

    public byte[] NonCoveredRangeStart { get; }

    public byte[] NonCoveredRangeEnd { get; }

    public FindTabletResult(RemoteTablet tablet, int index)
    {
        Tablet = tablet;
        Index = index;
        NonCoveredRangeStart = null;
        NonCoveredRangeEnd = null;
    }

    public FindTabletResult(byte[] nonCoveredRangeStart, byte[] nonCoveredRangeEnd)
    {
        Tablet = null;
        Index = -1;
        NonCoveredRangeStart = nonCoveredRangeStart;
        NonCoveredRangeEnd = nonCoveredRangeEnd;
    }

    public bool IsCoveredRange => Tablet is not null;

    public bool IsNonCoveredRange => Tablet is null;
}
