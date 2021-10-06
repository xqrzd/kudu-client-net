namespace Knet.Kudu.Client;

public class ColumnTypeAttributes
{
    public int? Precision { get; }

    public int? Scale { get; }

    public int? Length { get; }

    public ColumnTypeAttributes(int? precision, int? scale, int? length)
    {
        Precision = precision;
        Scale = scale;
        Length = length;
    }

    /// <summary>
    /// Return a string representation appropriate for `type`.
    /// This is meant to be postfixed to the name of a primitive type to
    /// describe the full type, e.g. decimal(10, 4).
    /// </summary>
    /// <param name="type">The data type.</param>
    public string ToStringForType(KuduType type)
    {
        switch (type)
        {
            case KuduType.Decimal32:
            case KuduType.Decimal64:
            case KuduType.Decimal128:
                return $"({Precision}, {Scale})";
            case KuduType.Varchar:
                return $"({Length})";
            default:
                return "";
        }
    }

    public static ColumnTypeAttributes NewDecimalAttributes(
        int precision, int scale)
    {
        return new ColumnTypeAttributes(precision, scale, null);
    }

    public static ColumnTypeAttributes NewVarcharAttributes(int length)
    {
        return new ColumnTypeAttributes(null, null, length);
    }
}
