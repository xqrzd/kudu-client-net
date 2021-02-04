using System;
using System.Collections.Generic;

namespace Knet.Kudu.Client.Tablet
{
    public static class RemoteTabletExtensions
    {
        public static RemoteTablet FindTablet(
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

                RemoteTablet tablet = tablets[i];
                int c = partitionKey.SequenceCompareTo(tablet.Partition.PartitionKeyStart);
                if (c == 0)
                {
                    return tablet;
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
                RemoteTablet tablet = tablets[hi];

                if (tablet.Partition.ContainsPartitionKey(partitionKey))
                {
                    return tablet;
                }
            }

            return null;
        }

        internal static byte[] GetNonCoveredRangeEnd(
            this List<RemoteTablet> tablets, ReadOnlySpan<byte> partitionKey)
        {
            // Inefficient search, but this is only used to display
            // more info in a NonCoveredRangeException.
            foreach (var tablet in tablets)
            {
                var start = tablet.Partition.PartitionKeyStart;

                if (partitionKey.SequenceCompareTo(start) < 0)
                    return start;
            }

            return Array.Empty<byte>();
        }
    }
}
