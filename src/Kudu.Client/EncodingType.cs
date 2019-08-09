using Kudu.Client.Protocol;

namespace Kudu.Client
{
    public enum EncodingType
    {
        AutoEncoding = EncodingTypePB.AutoEncoding,
        PlainEncoding = EncodingTypePB.PlainEncoding,
        PrefixEncoding = EncodingTypePB.PrefixEncoding,
        GroupVarint = EncodingTypePB.GroupVarint,
        Rle = EncodingTypePB.Rle,
        DictEncoding = EncodingTypePB.DictEncoding,
        BitShuffle = EncodingTypePB.BitShuffle
    }
}
