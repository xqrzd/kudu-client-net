using System.Collections.Generic;

namespace Knet.Kudu.Client;

public class HashBucketSchema
{
    public List<int> ColumnIds { get; }

    public int NumBuckets { get; }

    public uint Seed { get; }

    public HashBucketSchema(List<int> columnIds, int numBuckets, uint seed)
    {
        ColumnIds = columnIds;
        NumBuckets = numBuckets;
        Seed = seed;
    }
}
