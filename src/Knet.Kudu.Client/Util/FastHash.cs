using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Knet.Kudu.Client.Util;

/// <summary>
/// FastHash is simple, robust, and efficient general-purpose hash function from Google.
/// Implementation is adapted from https://code.google.com/archive/p/fast-hash/
/// </summary>
public static class FastHash
{
    // Help distinguish zero-length objects like empty strings.
    // These values must match those used by Apache Kudu.
    private static ReadOnlySpan<byte> HashValEmpty => new byte[] { 0xee, 0x7e, 0xca, 0x7d };

    /// <summary>
    /// Compute 64-bit FastHash.
    /// </summary>
    /// <param name="source">The data to hash.</param>
    /// <param name="seed">Seed to compute the hash.</param>
    public static ulong Hash64(ReadOnlySpan<byte> source, ulong seed)
    {
        if (source.Length == 0)
        {
            source = HashValEmpty;
        }

        uint length = (uint)source.Length;
        const ulong kMultiplier = 0x880355f21e6d1965;
        ulong h = seed ^ (length * kMultiplier);

        ReadOnlySpan<ulong> data = MemoryMarshal.Cast<byte, ulong>(source);

        foreach (ulong value in data)
        {
            h ^= FastHashMix(value);
            h *= kMultiplier;
        }

        ReadOnlySpan<byte> data2 = source.Slice(data.Length * sizeof(ulong));
        ulong v = 0;

        switch (length & 7)
        {
            case 7: v ^= (ulong)data2[6] << 48; goto case 6;
            case 6: v ^= (ulong)data2[5] << 40; goto case 5;
            case 5: v ^= (ulong)data2[4] << 32; goto case 4;
            case 4: v ^= (ulong)data2[3] << 24; goto case 3;
            case 3: v ^= (ulong)data2[2] << 16; goto case 2;
            case 2: v ^= (ulong)data2[1] << 8; goto case 1;
            case 1:
                v ^= data2[0];
                h ^= FastHashMix(v);
                h *= kMultiplier;
                break;
        }

        return FastHashMix(h);
    }

    /// <summary>
    /// Compute 32-bit FastHash.
    /// </summary>
    /// <param name="source">The data to hash.</param>
    /// <param name="seed">Seed to compute the hash.</param>
    public static uint Hash32(ReadOnlySpan<byte> source, uint seed)
    {
        // The following trick converts the 64-bit hashcode to Fermat
        // residue, which shall retain information from both the higher
        // and lower parts of hashcode.
        ulong h = Hash64(source, seed);
        return (uint)(h - (h >> 32));
    }

    /// <summary>
    /// Compression function for Merkle-Damgard construction.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong FastHashMix(ulong h)
    {
        h ^= h >> 23;
        h *= 0x2127599bf4325c37;
        h ^= h >> 47;
        return h;
    }
}
