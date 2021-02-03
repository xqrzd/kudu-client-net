#if !NETCOREAPP3_1 && !NET5_0
using System;

namespace Knet.Kudu.Client.Tablet
{
    public static partial class KeyEncoder
    {
        private static int EncodeBinary(
            ReadOnlySpan<byte> source, Span<byte> destination)
        {
            // In the common case where there are no zeros, this is
            // faster than copying byte by byte.
            int index = source.IndexOf((byte)0);

            if (index == -1)
            {
                // Data contains no zeros, do the fast path copy.
                source.CopyTo(destination);
                return source.Length;
            }
            else
            {
                return EncodeBinarySlow(source, destination);
            }
        }
    }
}
#endif
