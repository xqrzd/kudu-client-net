using Knet.Kudu.Client.Protobuf;

namespace Knet.Kudu.Client
{
    /// <summary>
    /// Supported Kudu data types.
    /// See https://kudu.apache.org/docs/schema_design.html#column-design
    /// </summary>
    public enum KuduType
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
        Decimal32 = DataTypePB.Decimal32,
        Decimal64 = DataTypePB.Decimal64,
        Decimal128 = DataTypePB.Decimal128,
        Varchar = DataTypePB.Varchar,
        Date = DataTypePB.Date
    }
}
