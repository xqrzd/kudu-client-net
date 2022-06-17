namespace Knet.Kudu.Client;

/// <summary>
/// Supported encoding for Kudu columns.
/// See https://kudu.apache.org/docs/schema_design.html#encoding
/// </summary>
public enum EncodingType
{
    AutoEncoding = Protobuf.EncodingType.AutoEncoding,
    PlainEncoding = Protobuf.EncodingType.PlainEncoding,
    PrefixEncoding = Protobuf.EncodingType.PrefixEncoding,
    Rle = Protobuf.EncodingType.Rle,
    DictEncoding = Protobuf.EncodingType.DictEncoding,
    BitShuffle = Protobuf.EncodingType.BitShuffle
}
