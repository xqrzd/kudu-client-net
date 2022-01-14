#if NETCOREAPP3_1_OR_GREATER
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Knet.Kudu.Client.Tablet;

public static partial class KeyEncoder
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EncodeBinary(
        ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (Sse41.IsSupported)
        {
            return EncodeBinarySse(source, destination);
        }

        return EncodeBinaryStandard(source, destination);
    }

    private static unsafe int EncodeBinarySse(
        ReadOnlySpan<byte> source, Span<byte> destination)
    {
        var length = (uint)source.Length;

        if ((uint)destination.Length < length * 2)
            ThrowException();

        fixed (byte* src = source)
        fixed (byte* dest = destination)
        {
            var srcCurrent = src;
            var destCurrent = dest;

            var end = src + length;
            var simdEnd = end - (length % 16);

            while (srcCurrent < simdEnd)
            {
                // Load 16 bytes (unaligned) into the XMM register.
                var data = Sse2.LoadVector128(srcCurrent);

                // Compare each byte of the input with '\0'. This results in a vector
                // where each byte is either \x00 or \xFF, depending on whether the
                // input had a '\x00' in the corresponding position.
                var zeroBytes = Sse2.CompareEqual(data, Vector128<byte>.Zero);

                // Check whether the resulting vector is all-zero.
                // If it's all zero, we can just store the entire chunk.
                if (Sse41.TestZ(zeroBytes, zeroBytes))
                {
                    Sse2.Store(destCurrent, data);
                }
                else
                {
                    break;
                }

                srcCurrent += 16;
                destCurrent += 16;
            }

            while (srcCurrent < end)
            {
                byte value = *srcCurrent++;
                if (value == 0)
                {
                    *destCurrent++ = 0;
                    *destCurrent++ = 1;
                }
                else
                {
                    *destCurrent++ = value;
                }
            }

            var written = destCurrent - dest;
            return (int)written;
        }
    }

    [DoesNotReturn]
    private static void ThrowException() =>
        throw new ArgumentException("Destination must be at least double source");
}
#endif
