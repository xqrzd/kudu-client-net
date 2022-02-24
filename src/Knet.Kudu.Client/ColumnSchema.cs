using System;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;

namespace Knet.Kudu.Client;

public sealed class ColumnSchema : IEquatable<ColumnSchema>
{
    public string Name { get; }

    public KuduType Type { get; }

    public bool IsKey { get; }

    public bool IsNullable { get; }

    public object? DefaultValue { get; }

    public int DesiredBlockSize { get; }

    public EncodingType Encoding { get; }

    public CompressionType Compression { get; }

    public ColumnTypeAttributes? TypeAttributes { get; }

    public int Size { get; }

    public bool IsSigned { get; }

    public bool IsFixedSize { get; }

    public string? Comment { get; }

    public ColumnSchema(
        string name, KuduType type,
        bool isKey = false,
        bool isNullable = false,
        object? defaultValue = null,
        int desiredBlockSize = 0,
        EncodingType encoding = EncodingType.AutoEncoding,
        CompressionType compression = CompressionType.DefaultCompression,
        ColumnTypeAttributes? typeAttributes = null,
        string? comment = null)
    {
        Name = name;
        Type = type;
        IsKey = isKey;
        IsNullable = isNullable;
        DefaultValue = defaultValue;
        DesiredBlockSize = desiredBlockSize;
        Encoding = encoding;
        Compression = compression;
        TypeAttributes = typeAttributes;
        Comment = comment;

        Size = KuduSchema.GetTypeSize(type);
        IsSigned = KuduSchema.IsSigned(type);
        IsFixedSize = IsTypeFixedSize(type);
    }

    public bool Equals(ColumnSchema? other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return
            Name == other.Name &&
            Type == other.Type &&
            IsKey == other.IsKey &&
            IsNullable == other.IsNullable &&
            TypeAttributes == other.TypeAttributes;
    }

    public override bool Equals(object? obj) => Equals(obj as ColumnSchema);

    public override int GetHashCode() => HashCode.Combine(Name, Type);

    public override string ToString()
    {
        var typeAttributes = TypeAttributes?.ToStringForType(Type);
        var nullable = IsNullable ? "?" : "";
        var key = IsKey ? " PK" : "";

        return $"{Name} {Type}{typeAttributes}{nullable}{key}";
    }

    public static bool operator ==(ColumnSchema? lhs, ColumnSchema? rhs)
    {
        if (lhs is null)
        {
            return rhs is null;
        }

        return lhs.Equals(rhs);
    }

    public static bool operator !=(ColumnSchema? lhs, ColumnSchema? rhs) => !(lhs == rhs);

    public static bool IsTypeFixedSize(KuduType type)
    {
        return
            type != KuduType.String &&
            type != KuduType.Binary &&
            type != KuduType.Varchar;
    }

    public static ColumnSchema FromProtobuf(ColumnSchemaPB columnSchemaPb)
    {
        var type = (KuduType)columnSchemaPb.Type;
        var typeAttributes = columnSchemaPb.TypeAttributes.ToTypeAttributes();
        var defaultValue = columnSchemaPb.HasWriteDefaultValue
            ? KuduEncoder.DecodeDefaultValue(
                type, typeAttributes, columnSchemaPb.WriteDefaultValue.Span)
            : null;

        return new ColumnSchema(
            columnSchemaPb.Name,
            type,
            columnSchemaPb.IsKey,
            columnSchemaPb.IsNullable,
            defaultValue,
            columnSchemaPb.CfileBlockSize,
            (EncodingType)columnSchemaPb.Encoding,
            (CompressionType)columnSchemaPb.Compression,
            typeAttributes,
            columnSchemaPb.Comment);
    }
}
