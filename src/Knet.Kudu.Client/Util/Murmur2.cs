using System;
using System.Runtime.InteropServices;

namespace Knet.Kudu.Client.Util;

/// <summary>
/// A C# implementation of the Murmur 2 hashing algorithm as presented at
/// https://sites.google.com/site/murmurhash.
///
/// Hash64 is from
/// https://sites.google.com/site/murmurhash/MurmurHash2_64.cpp
/// </summary>
public static class Murmur2
{
    /// <summary>
    /// Compute the Murmur2 hash (64-bit version) as described in the original source code.
    /// </summary>
    /// <param name="key">The data that needs to be hashed.</param>
    /// <param name="seed">The seed to use to compute the hash.</param>
    public static ulong Hash64(ReadOnlySpan<byte> key, ulong seed)
    {
        uint length = (uint)key.Length;
        const ulong m = 0xc6a4a7935bd1e995;
        const int r = 47;

        ulong h = seed ^ (length * m);

        var data = MemoryMarshal.Cast<byte, ulong>(key);

        foreach (ulong d in data)
        {
            ulong k = d;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        var data2 = key.Slice(data.Length * sizeof(ulong));

        switch (length & 7)
        {
            case 7: h ^= (ulong)data2[6] << 48; goto case 6;
            case 6: h ^= (ulong)data2[5] << 40; goto case 5;
            case 5: h ^= (ulong)data2[4] << 32; goto case 4;
            case 4: h ^= (ulong)data2[3] << 24; goto case 3;
            case 3: h ^= (ulong)data2[2] << 16; goto case 2;
            case 2: h ^= (ulong)data2[1] << 8; goto case 1;
            case 1:
                h ^= data2[0];
                h *= m;
                break;
        }

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }
}
