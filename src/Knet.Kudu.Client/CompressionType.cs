namespace Knet.Kudu.Client;

/// <summary>
/// Supported compression for Kudu columns.
/// See https://kudu.apache.org/docs/schema_design.html#compression
/// </summary>
public enum CompressionType
{
    DefaultCompression = Protobuf.CompressionType.DefaultCompression,
    NoCompression = Protobuf.CompressionType.NoCompression,
    Snappy = Protobuf.CompressionType.Snappy,
    Lz4 = Protobuf.CompressionType.Lz4,
    Zlib = Protobuf.CompressionType.Zlib
}
