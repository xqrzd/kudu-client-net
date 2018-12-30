using Kudu.Client.Protocol;

namespace Kudu.Client.Builder
{
    public enum DataType
    {
        UInt8 = DataTypePB.Uint8,
        Int8 = DataTypePB.Int8,
        UInt16 = DataTypePB.Uint16,
        Int16 = DataTypePB.Int16,
        UInt32 = DataTypePB.Uint32,
        Int32 = DataTypePB.Int32,
        UInt64 = DataTypePB.Uint64,
        Int64 = DataTypePB.Int64,
        String = DataTypePB.String,
        Bool = DataTypePB.Bool,
        Float = DataTypePB.Float,
        Double = DataTypePB.Double,
        Binary = DataTypePB.Binary,
        UnixtimeMicros = DataTypePB.UnixtimeMicros,
        //Int128 = DataTypePB.Int128, // Not supported in Kudu yet.
        Decimal32 = DataTypePB.Decimal32,
        Decimal64 = DataTypePB.Decimal64,
        Decimal128 = DataTypePB.Decimal128
    }
}
