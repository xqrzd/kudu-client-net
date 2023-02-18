using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests;

public class BlockBloomFilterTests
{
    /// <summary>
    /// We can construct Bloom filters with different spaces.
    /// </summary>
    [Fact]
    public void TestConstructor()
    {
        for (int i = 0; i < 30; i++)
        {
            _ = new BlockBloomFilter(i);
        }
    }

    [Fact]
    public void TestInvalidSpace()
    {
        int logSpaceBytes = Random.Shared.Next(38, 64);

        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => new BlockBloomFilter(logSpaceBytes));

        Assert.Contains("Bloom filter too large", exception.Message);
    }

    /// <summary>
    /// We can Insert() hashes into a Bloom filter with different spaces.
    /// </summary>
    [Fact]
    public void TestInsert()
    {
        for (int i = 13; i < 17; i++)
        {
            var bf = new BlockBloomFilter(i);
            for (int k = 0; k < (1 << 15); k++)
            {
                var hash = GetRandomHash();
                bf.Insert(hash);
            }
        }
    }

    /// <summary>
    /// After Insert()ing something into a Bloom filter, it can be found again immediately.
    /// </summary>
    [Fact]
    public void TestFind()
    {
        for (int i = 13; i < 17; i++)
        {
            var bf = new BlockBloomFilter(i);
            for (int k = 0; k < (1 << 15); k++)
            {
                var hash = GetRandomHash();
                bf.Insert(hash);
                Assert.True(bf.Find(hash));
            }
        }
    }

    /// <summary>
    /// After Insert()ing something into a Bloom filter, it can be found again much later.
    /// </summary>
    [Fact]
    public void TestCumulativeFind()
    {
        for (int i = 5; i < 11; i++)
        {
            var inserted = new List<uint>();
            var bf = new BlockBloomFilter(i);
            for (int k = 0; k < (1 << 10); k++)
            {
                var hash = GetRandomHash();
                inserted.Add(hash);
                bf.Insert(hash);

                foreach (var h in inserted)
                {
                    Assert.True(bf.Find(h));
                }
            }
        }
    }

    /// <summary>
    /// The empirical false positives we find when looking for random items is with a constant
    /// factor of the false positive probability the Bloom filter was constructed for.
    /// </summary>
    [Fact]
    public void TestFindInvalid()
    {
        // We use a deterministic pseudorandom number generator with a set seed. The reason is
        // that with a run-dependent seed, there will always be inputs that can fail. That's a
        // potential argument for this to be a benchmark rather than a test, although the
        // measured quantity would be not time but deviation from predicted fpp. 
        var random = new Random(867 + 5309);
        const int findLimit = 1 << 22;
        var toFind = new HashSet<uint>();
        while (toFind.Count < findLimit)
        {
            var hash = GetRandomHash(random);
            toFind.Add(hash);
        }
        const int maxLogNdv = 19;
        var toInsert = new HashSet<uint>();
        while (toInsert.Count < (1 << maxLogNdv))
        {
            var candidate = GetRandomHash(random);
            if (!toFind.Contains(candidate))
            {
                toInsert.Add(candidate);
            }
        }

        var shuffledInsert = new List<uint>(toInsert);

        for (int logNdv = 12; logNdv < maxLogNdv; logNdv++)
        {
            for (int logFpp = 4; logFpp < 12; logFpp++)
            {
                double fpp = 1.0 / (1 << logFpp);
                uint ndv = 1U << logNdv;
                int logHeapSpace = BlockBloomFilter.MinLogSpace(ndv, fpp);
                var bf = new BlockBloomFilter(logHeapSpace);
                // Fill up a BF with exactly as much ndv as we planned for it:
                for (int i = 0; i < ndv; i++)
                {
                    bf.Insert(shuffledInsert[i]);
                }

                // Now we sample from the set of possible hashes, looking for hits.
                int found = toFind.Where(bf.Find).Count();

                Assert.True(found <= findLimit * fpp * 2,
                    $"Too many false positives with -log2(fpp) = {fpp} and logNdv = {logNdv} " +
                    $"and logHeapSpace = {logHeapSpace}");

                // Because the space is rounded up to a power of 2, we might actually get a lower
                // fpp than the one passed to MinLogSpace().
                double expectedFpp = BlockBloomFilter.FalsePositiveProb(ndv, logHeapSpace);
                // Fudge factors are present because filter characteristics are true in the limit,
                // and will deviate for small samples.
                Assert.True(found >= findLimit * expectedFpp * 0.75,
                    $"Too few false positives with -log2(fpp) = {logFpp} expectedFpp = {expectedFpp}");

                Assert.True(found <= findLimit * expectedFpp * 1.25,
                    $"Too many false positives with -log2(fpp) = {fpp} expectedFpp = {expectedFpp}");
            }
        }
    }

    /// <summary>
    /// Test that MaxNdv() and MinLogSpace() are dual
    /// </summary>
    [Fact]
    public void TestMinSpaceMaxNdv()
    {
        for (int log2fpp = -2; log2fpp >= -30; log2fpp--)
        {
            double fpp = Math.Pow(2, log2fpp);
            for (int givenLogSpace = 8; givenLogSpace < 30; givenLogSpace++)
            {
                ulong derivedNdv = BlockBloomFilter.MaxNdv(givenLogSpace, fpp);
                // If NO values can be added without exceeding fpp, then the space needed is
                // trivially zero. This becomes a useless test; skip to the next iteration.
                if (0 == derivedNdv) continue;
                int derivedLogSpace = BlockBloomFilter.MinLogSpace(derivedNdv, fpp);

                Assert.Equal(derivedLogSpace, givenLogSpace);

                // If we lower the fpp, we need more space; if we raise it we need less.
                derivedLogSpace = BlockBloomFilter.MinLogSpace(derivedNdv, fpp / 2);
                Assert.True(derivedLogSpace >= givenLogSpace,
                    $"derivedLogSpace: {derivedLogSpace} givenLogSpace: {givenLogSpace} " +
                    $"fpp: {fpp} derivedNdv: {derivedNdv}");

                derivedLogSpace = BlockBloomFilter.MinLogSpace(derivedNdv, fpp * 2);
                Assert.True(derivedLogSpace <= givenLogSpace,
                    $"derivedLogSpace: {derivedLogSpace} givenLogSpace: {givenLogSpace} " +
                    $"fpp: {fpp} derivedNdv: {derivedNdv}");
            }
            for (uint givenNdv = 1000; givenNdv < 1000 * 1000; givenNdv *= 3)
            {
                int derivedLogSpace = BlockBloomFilter.MinLogSpace(givenNdv, fpp);
                ulong derivedNdv = BlockBloomFilter.MaxNdv(derivedLogSpace, fpp);

                // The max ndv is close to, but larger than, then ndv we asked for
                Assert.True(givenNdv <= derivedNdv,
                    $"givenNdv: {givenNdv} derivedNdv: {derivedNdv} fpp: {fpp} derivedLogSpace: {derivedLogSpace}");
                Assert.True(givenNdv * 2 >= derivedNdv,
                    $"givenNdv: {givenNdv} derivedNdv: {derivedNdv} fpp: {fpp} derivedLogSpace: {derivedLogSpace}");

                // Changing the fpp changes the ndv capacity in the expected direction.
                ulong newDerivedNdv = BlockBloomFilter.MaxNdv(derivedLogSpace, fpp / 2);
                Assert.True(derivedNdv >= newDerivedNdv,
                    $"derivedNdv: {derivedNdv} newDerivedNdv: {newDerivedNdv} " +
                    $"fpp: {fpp} derivedLogSpace: {derivedLogSpace}");

                newDerivedNdv = BlockBloomFilter.MaxNdv(derivedLogSpace, fpp * 2);
                Assert.True(derivedNdv <= newDerivedNdv,
                    $"derivedNdv: {derivedNdv} newDerivedNdv: {newDerivedNdv} " +
                    $"fpp: {fpp} derivedLogSpace: {derivedLogSpace}");
            }
        }
    }

    [Fact]
    public void TestMinSpaceEdgeCase()
    {
        int minSpace = BlockBloomFilter.MinLogSpace(1, 0.75);
        Assert.True(minSpace >= 0, "LogSpace should always be >= 0");
    }

    /// <summary>
    /// Check that MinLogSpace() and FalsePositiveProb() are dual
    /// </summary>
    [Fact]
    public void TestMinSpaceForFpp()
    {
        for (ulong ndv = 10000; ndv < 100 * 1000 * 1000; ndv = (ulong)(ndv * 1.1))
        {
            for (double fpp = 0.1; fpp > Math.Pow(2, -20); fpp *= 0.9)
            {
                // When contructing a Bloom filter, we can request a particular fpp by calling
                // MinLogSpace().
                int minLogSpace = BlockBloomFilter.MinLogSpace(ndv, fpp);
                // However, at the resulting ndv and space, the expected fpp might be lower than
                // the one that was requested.
                double expectedFpp = BlockBloomFilter.FalsePositiveProb(ndv, minLogSpace);
                Assert.True(expectedFpp <= fpp, $"expectedFpp: {expectedFpp} fpp: {fpp}");
                // The fpp we get might be much lower than the one we asked for. However, if the
                // space were just one size smaller, the fpp we get would be larger than the one we
                // asked for.
                expectedFpp = BlockBloomFilter.FalsePositiveProb(ndv, minLogSpace - 1);
                Assert.True(expectedFpp >= fpp, $"expectedFpp: {expectedFpp} fpp: {fpp}");
                // Therefore, the return value of MinLogSpace() is actually the minimum
                // log space at which we can guarantee the requested fpp.
            }
        }
    }

    private static uint GetRandomHash() => GetRandomHash(Random.Shared);

    private static uint GetRandomHash(Random random)
    {
        Span<byte> bytes = stackalloc byte[4];
        random.NextBytes(bytes);
        return BinaryPrimitives.ReadUInt32LittleEndian(bytes);
    }
}
