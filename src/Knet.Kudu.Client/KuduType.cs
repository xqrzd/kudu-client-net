using Knet.Kudu.Client.Protobuf;

namespace Knet.Kudu.Client;

/// <summary>
/// Supported Kudu data types.
/// See https://kudu.apache.org/docs/schema_design.html#column-design
/// </summary>
public enum KuduType
{
    Int8 = DataType.Int8,
    Int16 = DataType.Int16,
    Int32 = DataType.Int32,
    Int64 = DataType.Int64,
    String = DataType.String,
    Bool = DataType.Bool,
    Float = DataType.Float,
    Double = DataType.Double,
    Binary = DataType.Binary,
    UnixtimeMicros = DataType.UnixtimeMicros,
    Decimal32 = DataType.Decimal32,
    Decimal64 = DataType.Decimal64,
    Decimal128 = DataType.Decimal128,
    Varchar = DataType.Varchar,
    Date = DataType.Date
}
