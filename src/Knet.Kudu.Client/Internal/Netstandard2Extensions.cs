#if NETSTANDARD2_0

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.Internal;

internal static class Netstandard2Extensions
{
    public static async ValueTask WriteAsync(
        this Stream stream,
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        await stream.WriteAsync(buffer.ToArray(), 0, buffer.Length, cancellationToken)
            .ConfigureAwait(false);
    }

    public static async ValueTask<int> ReadAsync(
        this Stream stream,
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        var tempBuffer = new byte[buffer.Length];
        var read = await stream.ReadAsync(tempBuffer, 0, tempBuffer.Length, cancellationToken)
            .ConfigureAwait(false);

        tempBuffer.AsMemory(0, read).CopyTo(buffer);
        return read;
    }

    public static void Write(this Stream stream, ReadOnlySpan<byte> buffer)
    {
        stream.Write(buffer.ToArray(), 0, buffer.Length);
    }

    public static int Read(this Stream stream, Span<byte> buffer)
    {
        var tempBuffer = new byte[buffer.Length];
        var read = stream.Read(tempBuffer, 0, tempBuffer.Length);
        tempBuffer.AsSpan(0, read).CopyTo(buffer);
        return read;
    }

    public static string GetString(this Encoding encoding, ReadOnlySpan<byte> bytes)
    {
        return encoding.GetString(bytes.ToArray());
    }

    public static int GetBytes(this Encoding encoding, string s, Span<byte> bytes)
    {
        var result = encoding.GetBytes(s);
        result.CopyTo(bytes);
        return result.Length;
    }

    public static bool Remove<TKey, TValue>(
        this Dictionary<TKey, TValue> dictionary, TKey key, out TValue value)
    {
        dictionary.TryGetValue(key, out value);
        return dictionary.Remove(key);
    }

    public static TValue GetOrAdd<TKey, TValue, TArg>(
        this ConcurrentDictionary<TKey, TValue> dictionary,
        TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument)
    {
        return dictionary.GetOrAdd(key, key => valueFactory(key, factoryArgument));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe int SingleToInt32Bits(float value)
    {
        return *(int*)&value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe float Int32BitsToSingle(int value)
    {
        return *(float*)&value;
    }
}

#endif
