#if !NETCOREAPP3_1_OR_GREATER
using System;

namespace Knet.Kudu.Client.Tablet;

public static partial class KeyEncoder
{
    private static int EncodeBinary(
        ReadOnlySpan<byte> source, Span<byte> destination)
    {
        return EncodeBinaryStandard(source, destination);
    }
}
#endif
