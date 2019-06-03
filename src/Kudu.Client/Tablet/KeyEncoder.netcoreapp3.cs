#if NETCOREAPP3_0
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Kudu.Client.Tablet
{
    public static partial class KeyEncoder
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int EncodeBinary(
            ReadOnlySpan<byte> source, Span<byte> destination)
        {
            if (Avx.IsSupported)
            {
                return EncodeBinaryAvx(source, destination);
            }
            else if (Sse.IsSupported)
            {
                return EncodeBinarySse(source, destination);
            }
            else
            {
                return EncodeBinarySlow(source, destination);
            }
        }

        private static unsafe int EncodeBinaryAvx(
            ReadOnlySpan<byte> source, Span<byte> destination)
        {
            if (destination.Length < source.Length * 2)
                ThrowException();

            int i = 0;

            fixed (byte* src = source)
            fixed (byte* dest = destination)
            {
                var srcCurrent = src;
                var destCurrent = dest;

                for (; i < source.Length; i += 32)
                {
                    var data = Avx.LoadVector256(srcCurrent);
                    var zeros = new Vector256<byte>();

                    var zeroBytes = Avx2.CompareEqual(data, zeros);
                    bool allZeros = Avx.TestZ(zeroBytes, zeroBytes);

                    if (allZeros)
                        Avx.Store(destCurrent, data);
                    else
                        break;

                    srcCurrent += 32;
                    destCurrent += 32;
                }

                for (; i < source.Length; i += 16)
                {
                    var data = Sse2.LoadVector128(srcCurrent);
                    var zeros = new Vector128<byte>();

                    var zeroBytes = Sse2.CompareEqual(data, zeros);
                    bool allZeros = Sse41.TestZ(zeroBytes, zeroBytes);

                    if (allZeros)
                        Sse2.Store(destCurrent, data);
                    else
                        break;

                    srcCurrent += 16;
                    destCurrent += 16;
                }

                int extra = 0;
                for (; i < source.Length; i++)
                {
                    byte value = *srcCurrent++;
                    if (value == 0)
                    {
                        *destCurrent++ = 0;
                        *destCurrent++ = 1;
                        extra++;
                    }
                    else
                    {
                        *destCurrent++ = value;
                    }
                }
                i += extra;
            }

            return i;
        }

        private static unsafe int EncodeBinarySse(
            ReadOnlySpan<byte> source, Span<byte> destination)
        {
            if (destination.Length < source.Length * 2)
                ThrowException();

            int i = 0;

            fixed (byte* src = source)
            fixed (byte* dest = destination)
            {
                var srcCurrent = src;
                var destCurrent = dest;

                for (; i < source.Length; i += 16)
                {
                    var data = Sse2.LoadVector128(srcCurrent);
                    var zeros = new Vector128<byte>();

                    var zeroBytes = Sse2.CompareEqual(data, zeros);
                    bool allZeros = Sse41.TestZ(zeroBytes, zeroBytes);

                    if (allZeros)
                        Sse2.Store(destCurrent, data);
                    else
                        break;

                    srcCurrent += 16;
                    destCurrent += 16;
                }

                int extra = 0;
                for (; i < source.Length; i++)
                {
                    byte value = *srcCurrent++;
                    if (value == 0)
                    {
                        *destCurrent++ = 0;
                        *destCurrent++ = 1;
                        extra++;
                    }
                    else
                    {
                        *destCurrent++ = value;
                    }
                }
                i += extra;
            }

            return i;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowException() =>
            throw new Exception("Destination must be at least double source");
    }
}
#endif
