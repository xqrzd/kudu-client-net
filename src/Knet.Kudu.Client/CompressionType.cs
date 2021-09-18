using Knet.Kudu.Client.Protobuf;

namespace Knet.Kudu.Client;

/// <summary>
/// Supported compression for Kudu columns.
/// See https://kudu.apache.org/docs/schema_design.html#compression
/// </summary>
public enum CompressionType
{
    DefaultCompression = CompressionTypePB.DefaultCompression,
    NoCompression = CompressionTypePB.NoCompression,
    Snappy = CompressionTypePB.Snappy,
    Lz4 = CompressionTypePB.Lz4,
    Zlib = CompressionTypePB.Zlib
}
