using Kudu.Client.Protocol;

namespace Kudu.Client.Builder
{
    public enum DataType
    {
        Int8 = DataTypePB.Int8,
        Int16 = DataTypePB.Int16,
        Int32 = DataTypePB.Int32,
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
