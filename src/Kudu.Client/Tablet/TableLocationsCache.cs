using System;
using System.Collections.Generic;

namespace Kudu.Client.Tablet
{
    public class TableLocationsCache
    {
        // TODO: Replace this with a tree, or at least a sorted list with binarch search.
        // This implementation is extremely inefficient.
        private readonly List<RemoteTablet> _tablets;

        public TableLocationsCache(List<RemoteTablet> tablets)
        {
            _tablets = tablets;
        }

        public RemoteTablet FindTablet(ReadOnlySpan<byte> partitionKey)
        {
            for (int i = 0; i < _tablets.Count; i++)
            {
                var start = _tablets[i].Partition.PartitionKeyStart;
                var end = _tablets[i].Partition.PartitionKeyEnd;
                var isEnd = _tablets[i].Partition.IsEndPartition;

                var compareStart = partitionKey.SequenceCompareTo(start);
                var compareEnd = partitionKey.SequenceCompareTo(end);

                if (compareStart >= 0 && (compareEnd < 0 || isEnd))
                {
                    return _tablets[i];
                }
            }

            throw new Exception("Unable to find tablet");
        }
    }
}
