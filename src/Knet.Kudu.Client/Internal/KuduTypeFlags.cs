using System;

namespace Knet.Kudu.Client.Internal;

[Flags]
internal enum KuduTypeFlags
{
    Int8 = 1 << KuduType.Int8,
    Int16 = 1 << KuduType.Int16,
    Int32 = 1 << KuduType.Int32,
    Int64 = 1 << KuduType.Int64,
    String = 1 << KuduType.String,
    Bool = 1 << KuduType.Bool,
    Float = 1 << KuduType.Float,
    Double = 1 << KuduType.Double,
    Binary = 1 << KuduType.Binary,
    UnixtimeMicros = 1 << KuduType.UnixtimeMicros,
    Decimal32 = 1 << KuduType.Decimal32,
    Decimal64 = 1 << KuduType.Decimal64,
    Decimal128 = 1 << KuduType.Decimal128,
    Varchar = 1 << KuduType.Varchar,
    Date = 1 << KuduType.Date
}
