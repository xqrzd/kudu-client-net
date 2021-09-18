using Knet.Kudu.Client.Protobuf;

namespace Knet.Kudu.Client;

/// <summary>
/// Supported encoding for Kudu columns.
/// See https://kudu.apache.org/docs/schema_design.html#encoding
/// </summary>
public enum EncodingType
{
    AutoEncoding = EncodingTypePB.AutoEncoding,
    PlainEncoding = EncodingTypePB.PlainEncoding,
    PrefixEncoding = EncodingTypePB.PrefixEncoding,
    Rle = EncodingTypePB.Rle,
    DictEncoding = EncodingTypePB.DictEncoding,
    BitShuffle = EncodingTypePB.BitShuffle
}
