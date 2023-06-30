using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Knet.Kudu.Client.Internal;
#if NET7_0_OR_GREATER
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
#endif

namespace Knet.Kudu.Client.Util;

public class BlockBloomFilter : IEquatable<BlockBloomFilter>
{
    /// <summary>
    /// Rehash is used as 8 odd 32-bit unsigned ints. See Dietzfelbinger et al.'s
    /// "A reliable randomized algorithm for the closest-pair problem".
    /// </summary>
    private static readonly uint[] _rehash = new uint[]
    {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
        0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    /// <summary>
    /// Lanczos approximation g=5, n=7
    /// </summary>
    private static readonly double[] _lgammaCoef = new double[]
    {
        1.000000000190015,
        76.18009172947146, -86.50532032941677,
        24.01409824083091, -1.231739572450155,
        0.1208650973866179e-2, -0.5395239384953e-5
    };

    /// <summary>
    /// The BloomFilter is divided up into Buckets and each Bucket comprises of 8 BucketWords
    /// of 4 bytes each.
    /// </summary>
    private const int BucketWords = 8;

    /// <summary>
    /// log2(number of bits in a BucketWord)
    /// </summary>
    private const int LogBucketWordBits = 5;
    private const uint BucketWordMask = (1 << LogBucketWordBits) - 1;

    /// <summary>
    /// log2(number of bytes in a bucket)
    /// </summary>
    private const int LogBucketByteSize = 5;

    /// <summary>
    /// Bucket size in bytes.
    /// </summary>
    private const int BucketByteSize = 1 << LogBucketByteSize;

    /// <summary>
    /// The log (base 2) of the number of buckets in the directory.
    /// </summary>
    private readonly int _logNumBuckets;

    /// <summary>
    /// (1 &lt;&lt; LogNumBuckets) - 1. It is precomputed for efficiency reasons.
    /// </summary>
    private readonly uint _directoryMask;

    private readonly byte[] _directory;

    public BlockBloomFilter(int logSpaceBytes) : this(logSpaceBytes, null, true)
    {
    }

    public BlockBloomFilter(int logSpaceBytes, byte[]? directory, bool alwaysFalse)
    {
        // Since logSpaceBytes is in bytes, we need to convert it to the number of tiny
        // Bloom filters we will use.
        _logNumBuckets = Math.Max(1, logSpaceBytes - LogBucketByteSize);

        // Since we use 32 bits in the arguments of Insert() and Find(), _logNumBuckets
        // must be limited. Don't use _logNumBuckets if it will lead to undefined behavior
        // by a shift that is too large.
        if (_logNumBuckets > 32)
        {
            throw new ArgumentOutOfRangeException(nameof(logSpaceBytes),
                $"Bloom filter too large. logSpaceBytes: {logSpaceBytes}");
        }

        _directoryMask = (1u << _logNumBuckets) - 1;

        var directorySize = 1 << LogSpaceBytes;
        if (directory is null)
        {
            _directory = new byte[directorySize];
        }
        else
        {
            if (directory.Length != directorySize)
            {
                throw new ArgumentException($"Mismatch in BlockBloomFilter source directory " +
                    $"size {directory.Length} and expected size {directorySize}");
            }

            _directory = directory;
        }

        AlwaysFalse = alwaysFalse;
    }

    /// <summary>
    /// Returns whether the Bloom filter is empty and hence would return false for all lookups.
    /// </summary>
    public bool AlwaysFalse { get; private set; }

    /// <summary>
    /// Returns amount of space used in log2 bytes.
    /// </summary>
    public int LogSpaceBytes => _logNumBuckets + LogBucketByteSize;

    public ReadOnlySpan<byte> Span => _directory;

    public ReadOnlyMemory<byte> Memory => _directory;

    public void Insert(uint hash)
    {
        AlwaysFalse = false;
        uint bucketIndex = Rehash32to32(hash) & _directoryMask;

#if NET7_0_OR_GREATER
        if (Avx2.IsSupported)
        {
            ref byte bucketRef = ref GetBucketReference(bucketIndex);

            Vector256<uint> mask = MakeMaskAvx2(hash);
            Vector256<uint> bucket = Unsafe.ReadUnaligned<Vector256<uint>>(ref bucketRef);

            var result = Vector256.BitwiseOr(bucket, mask).AsByte();

            result.StoreUnsafe(ref bucketRef);
        }
        else
        {
            InsertBucket(bucketIndex, hash);
        }
#else
        InsertBucket(bucketIndex, hash);
#endif
    }

    private void InsertBucket(uint bucketIndex, uint hash)
    {
        // newBucket will be all zeros except for eight 1-bits, one in each 32-bit word.
        Span<uint> newBucket = stackalloc uint[BucketWords];

        for (int i = 0; i < BucketWords; i++)
        {
            newBucket[i] = MakeMask(hash, i);
        }

#if NET7_0_OR_GREATER
        if (Vector128.IsHardwareAccelerated)
        {
            ref byte newBucketRef = ref Unsafe.As<uint, byte>(ref MemoryMarshal.GetReference(newBucket));
            ref byte existingBucketRef = ref GetBucketReference(bucketIndex);

            for (nuint i = 0; i < 2; i++)
            {
                var newBucketVector = Vector128.LoadUnsafe(ref newBucketRef);
                var existingBucket = Unsafe.ReadUnaligned<Vector128<byte>>(ref existingBucketRef);

                var result = Vector128.BitwiseOr(existingBucket, newBucketVector);

                Unsafe.WriteUnaligned(ref existingBucketRef, result);

                newBucketRef = ref Unsafe.Add(ref newBucketRef, Vector128<byte>.Count);
                existingBucketRef = ref Unsafe.Add(ref existingBucketRef, Vector128<byte>.Count);
            }
        }
        else
        {
            InsertBucket(bucketIndex, newBucket);
        }
#else
        InsertBucket(bucketIndex, newBucket);
#endif
    }

    private void InsertBucket(uint bucketIndex, Span<uint> newBucket)
    {
        Span<uint> existingBucket = GetBucketSpan(bucketIndex);

        for (int i = 0; i < BucketWords; i++)
        {
            existingBucket[i] |= newBucket[i];
        }
    }

    public bool Find(uint hash)
    {
        if (AlwaysFalse)
            return false;

        uint bucketIndex = Rehash32to32(hash) & _directoryMask;

#if NET7_0_OR_GREATER
        if (Avx2.IsSupported)
        {
            Vector256<uint> mask = MakeMaskAvx2(hash);
            Vector256<uint> bucket = Unsafe.ReadUnaligned<Vector256<uint>>(ref GetBucketReference(bucketIndex));
            // We should return true if 'bucket' has a one wherever 'mask' does. _mm256_testc_si256
            // takes the negation of its first argument and ands that with its second argument. In
            // our case, the result is zero everywhere iff there is a one in 'bucket' wherever
            // 'mask' is one. testc returns 1 if the result is 0 everywhere and returns 0 otherwise.
            return Avx.TestC(bucket, mask);
        }
        else
        {
            return Find(bucketIndex, hash);
        }
#else
        return Find(bucketIndex, hash);
#endif
    }

    private bool Find(uint bucketIndex, uint hash)
    {
        Span<uint> bucket = GetBucketSpan(bucketIndex);

        for (int i = 0; i < BucketWords; i++)
        {
            uint mask = MakeMask(hash, i);

            if ((bucket[i] & mask) == 0)
            {
                return false;
            }
        }

        return true;
    }

    public bool Equals(BlockBloomFilter? other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return
            AlwaysFalse == other.AlwaysFalse &&
            Span.SequenceEqual(other.Span);
    }

    public override bool Equals(object? obj) => Equals(obj as BlockBloomFilter);

    public override int GetHashCode() => _directory.GetContentHashCode();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Span<uint> GetBucketSpan(uint bucketIndex)
    {
        int offset = (int)bucketIndex * BucketByteSize;
        var slice = _directory.AsSpan(offset, BucketByteSize);
        return MemoryMarshal.Cast<byte, uint>(slice);
    }

#if NET6_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref byte GetBucketReference(uint bucketIndex)
    {
        ref byte directory = ref MemoryMarshal.GetArrayDataReference(_directory);
        return ref Unsafe.Add(ref directory, bucketIndex * BucketByteSize);
    }
#endif

    /// <summary>
    /// Returns the expected false positive rate for the given ndv and logSpaceBytes.
    /// </summary>
    /// <param name="ndv">Number of distinct values.</param>
    /// <param name="logSpaceBytes">Space used in log2 bytes.</param>
    public static double FalsePositiveProb(ulong ndv, int logSpaceBytes)
    {
        const double wordBits = 1 << LogBucketWordBits;
        double bytes = 1L << logSpaceBytes;
        if (ndv == 0) return 0.0;
        if (bytes <= 0) return 1.0;

        // This short-cuts a slowly-converging sum for very dense filters
        if (ndv / (bytes * 8) > 2) return 1.0;

        double result = 0;
        // lam is the usual parameter to the Poisson's PMF. Following the notation in the paper,
        // lam is B/c, where B is the number of bits in a bucket and c is the number of bits per
        // distinct value
        double lam = BucketWords * wordBits / ((bytes * 8) / ndv);
        // Some of the calculations are done in log-space to increase numerical stability
        double loglam = Math.Log(lam);

        // 750 iterations are sufficient to cause the sum to converge in all of the tests. In
        // other words, setting the iterations higher than 750 will give the same result as
        // leaving it at 750.
        const ulong bloomFppIters = 750;

        for (ulong j = 0; j < bloomFppIters; j++)
        {
            // We start with the highest value of i, since the values we're adding to result are
            // mostly smaller at high i, and this increases accuracy to sum from the smallest
            // values up.
            double i = bloomFppIters - 1 - j;
            // The PMF of the Poisson distribution is lam^i * exp(-lam) / i!. In logspace, using
            // lgamma for the log of the factorial function:
            double logp = i * loglam - lam - LogGamma(i + 1);
            // The f_inner part of the equation in the paper is the probability of a single
            // collision in the bucket. Since there are BucketWords non-overlapping lanes in each
            // bucket, the log of this probability is:
            double logfinner = BucketWords * Math.Log(1.0 - Math.Pow(1.0 - 1.0 / wordBits, i));
            // Here we are forced out of log-space calculations
            result += Math.Exp(logp + logfinner);
        }

        return (result > 1.0) ? 1.0 : result;
    }

    /// <summary>
    /// As more distinct items are inserted into a BloomFilter, the false positive rate
    /// rises. MaxNdv() returns the NDV (number of distinct values) at which a BloomFilter
    /// constructed with (1 &lt;&lt; logSpaceBytes) bytes of space hits false positive
    /// probability fpp.
    /// </summary>
    /// <param name="logSpaceBytes">Space used in log2 bytes.</param>
    /// <param name="fpp">False positive probability.</param>
    public static ulong MaxNdv(int logSpaceBytes, double fpp)
    {
        // Starting with an exponential search, we find bounds for how many distinct values a
        // filter of size (1 << logSpaceBytes) can hold before it exceeds a false positive
        // probability of fpp.
        ulong tooMany = 1;
        while (FalsePositiveProb(tooMany, logSpaceBytes) <= fpp)
        {
            tooMany *= 2;
        }
        // From here forward, we have the invariant that FalsePositiveProb(tooMany,
        // logSpaceBytes) > fpp
        ulong tooFew = tooMany / 2;
        // Invariant for tooFew: FalsePositiveProb(tooFew, logSpaceBytes) <= fpp

        const uint proximity = 1;
        // Simple binary search. If this is too slow, the required proximity of tooFew and
        // tooMany can be raised from 1 to something higher.
        while (tooMany - tooFew > proximity)
        {
            ulong mid = (tooMany + tooFew) / 2;
            double midFpp = FalsePositiveProb(mid, logSpaceBytes);
            if (midFpp <= fpp)
            {
                tooFew = mid;
            }
            else
            {
                tooMany = mid;
            }
        }

        return tooFew;
    }

    /// <summary>
    /// If we expect to fill a Bloom filter with 'ndv' different unique elements and we
    /// want a false positive probability of less than 'fpp', then this is the log (base 2)
    /// of the minimum number of bytes we need.
    /// </summary>
    /// <param name="ndv">Number of distinct values.</param>
    /// <param name="fpp">False positive probability.</param>
    public static int MinLogSpace(ulong ndv, double fpp)
    {
        int low = 0;
        int high = 64;
        while (high > low + 1)
        {
            int mid = (high + low) / 2;
            double candidate = FalsePositiveProb(ndv, mid);
            if (candidate <= fpp)
            {
                high = mid;
            }
            else
            {
                low = mid;
            }
        }
        return high;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Rehash32to32(uint hash)
    {
        // Constants generated by uuidgen(1) with the -r flag
        const ulong m = 0x7850f11ec6d14889;
        const ulong a = 0x6773610597ca4c63;
        // This is strongly universal hashing following Dietzfelbinger's "Universal hashing
        // and k-wise independent random variables via integer arithmetic without primes". As
        // such, for any two distinct uint32_t's hash1 and hash2, the probability (over the
        // randomness of the constants) that any subset of bit positions of
        // Rehash32to32(hash1) is equal to the same subset of bit positions
        // Rehash32to32(hash2) is minimal.
        return (uint)((hash * m + a) >> 32);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint MakeMask(uint hash, int i)
    {
        // Rehash 'hash' and use the top LogBucketWordBits bits, following Dietzfelbinger.
        uint hval = (_rehash[i] * hash) >> ((1 << LogBucketWordBits) - LogBucketWordBits);
        return 1u << (int)hval;
    }

#if NET7_0_OR_GREATER
    /// <summary>
    /// Turns a 32-bit hash into a 256-bit Bucket with 1 single 1-bit set in each 32-bit lane.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> MakeMaskAvx2(uint hash)
    {
        var ones = Vector256.Create<uint>(1);
        var rehash = Vector256.Create(_rehash);

        // Load hash into a YMM register, repeated eight times
        var hashData = Vector256.Create(hash);

        // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
        // odd constants, then keep the 5 most significant bits from each product.
        hashData = Vector256.Multiply(rehash, hashData);
        hashData = Vector256.ShiftRightLogical(hashData, 27);

        // Use these 5 bits to shift a single bit to a location in each 32-bit lane
        return Avx2.ShiftLeftLogicalVariable(ones, hashData);
    }
#endif

    // https://visualstudiomagazine.com/articles/2022/08/02/logbeta-loggamma-functions-csharp.aspx
    private static double LogGamma(double z)
    {
        const double LogSqrtTwoPi = 0.91893853320467274178;
        var coef = _lgammaCoef;

        if (z < 0.5)
            return Math.Log(Math.PI / Math.Sin(Math.PI * z)) - LogGamma(1.0 - z);

        double zz = z - 1.0;
        double b = zz + 5.5; // g + 0.5
        double sum = coef[0];

        for (int i = 1; i < coef.Length; i++)
            sum += coef[i] / (zz + i);

        return (LogSqrtTwoPi + Math.Log(sum) - b) + (Math.Log(b) * (zz + 0.5));
    }
}
