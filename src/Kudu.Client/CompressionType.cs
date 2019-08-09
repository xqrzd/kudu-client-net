using Kudu.Client.Protocol;

namespace Kudu.Client
{
    public enum CompressionType
    {
        DefaultCompression = CompressionTypePB.DefaultCompression,
        NoCompression = CompressionTypePB.NoCompression,
        Snappy = CompressionTypePB.Snappy,
        Lz4 = CompressionTypePB.Lz4,
        Zlib = CompressionTypePB.Zlib
    }
}
