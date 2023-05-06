using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client;

/// <summary>
/// Builder class to help build <see cref="KuduBloomFilter"/> to be used with
/// IN Bloom filter predicates.
/// </summary>
public class KuduBloomFilterBuilder
{
    private readonly ColumnSchema _column;
    private readonly ulong _numKeys;
    private uint _hashSeed = 0;
    private double _fpp = 0.01;

    /// <param name="column">The column schema.</param>
    /// <param name="numKeys">Expected number of unique elements to be inserted in the Bloom filter.</param>
    public KuduBloomFilterBuilder(ColumnSchema column, ulong numKeys)
    {
        _column = column;
        _numKeys = numKeys;
    }

    /// <summary>
    /// Seed used with hash algorithm to hash the keys before inserting to
    /// the Bloom filter. If not provided, defaults to 0.
    /// </summary>
    public KuduBloomFilterBuilder SetHashSeed(uint seed)
    {
        _hashSeed = seed;
        return this;
    }

    /// <summary>
    /// Desired false positive probability between 0.0 and 1.0.
    /// If not provided, defaults to 0.01.
    /// </summary>
    public KuduBloomFilterBuilder SetFalsePositiveProbability(double fpp)
    {
        _fpp = fpp;
        return this;
    }

    public KuduBloomFilter Build()
    {
        int logSpaceBytes = BlockBloomFilter.MinLogSpace(_numKeys, _fpp);
        var blockBloomFilter = new BlockBloomFilter(logSpaceBytes);

        return new KuduBloomFilter(blockBloomFilter, _hashSeed, _column);
    }
}
