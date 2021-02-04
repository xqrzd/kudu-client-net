#if NETSTANDARD2_0 || NETSTANDARD2_1
using System;

namespace Knet.Kudu.Client.Tablet
{
    public static partial class KeyEncoder
    {
        private static int EncodeBinary(
            ReadOnlySpan<byte> source, Span<byte> destination)
        {
            return EncodeBinaryStandard(source, destination);
        }
    }
}
#endif
