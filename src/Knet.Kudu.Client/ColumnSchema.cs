using System;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client
{
    public class ColumnSchema : IEquatable<ColumnSchema>
    {
        public string Name { get; }

        public KuduType Type { get; }

        public bool IsKey { get; }

        public bool IsNullable { get; }

        public EncodingType Encoding { get; }

        public CompressionType Compression { get; }

        public ColumnTypeAttributes TypeAttributes { get; }

        public int Size { get; }

        public bool IsSigned { get; }

        public bool IsFixedSize { get; }

        public ColumnSchema(
            string name, KuduType type,
            bool isKey = false,
            bool isNullable = false,
            EncodingType encoding = EncodingType.AutoEncoding,
            CompressionType compression = CompressionType.DefaultCompression,
            ColumnTypeAttributes typeAttributes = null)
        {
            Name = name;
            Type = type;
            IsKey = isKey;
            IsNullable = isNullable;
            Encoding = encoding;
            Compression = compression;
            TypeAttributes = typeAttributes;

            Size = KuduSchema.GetTypeSize(type);
            IsSigned = KuduSchema.IsSigned(type);
            IsFixedSize = !(type == KuduType.String || type == KuduType.Binary);
        }

        public bool Equals(ColumnSchema other)
        {
            if (other is null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return
                Name == other.Name &&
                Type == other.Type &&
                IsKey == other.IsKey &&
                IsNullable == other.IsNullable;
        }

        public override bool Equals(object obj) => Equals(obj as ColumnSchema);

        public override int GetHashCode() => HashCode.Combine(Name, Type);

        public override string ToString()
        {
            var typeAttributes = TypeAttributes == null ? "" :
                $"({TypeAttributes.Precision}, {TypeAttributes.Scale})";

            var nullable = IsNullable ? "?" : "";
            var key = IsKey ? " PK" : "";

            return $"{Name} {Type}{typeAttributes}{nullable}{key}";
        }

        public static ColumnSchema FromProtobuf(ColumnSchemaPB columnSchemaPb)
        {
            return new ColumnSchema(
                columnSchemaPb.Name,
                (KuduType)columnSchemaPb.Type,
                columnSchemaPb.IsKey,
                columnSchemaPb.IsNullable,
                (EncodingType)columnSchemaPb.Encoding,
                (CompressionType)columnSchemaPb.Compression,
                FromProtobuf(columnSchemaPb.TypeAttributes));
        }

        private static ColumnTypeAttributes FromProtobuf(
            ColumnTypeAttributesPB typeAttributesPb)
        {
            if (typeAttributesPb is null)
                return null;

            return new ColumnTypeAttributes(
                typeAttributesPb.Precision,
                typeAttributesPb.Scale);
        }
    }

    public class ColumnTypeAttributes
    {
        public int Precision { get; }

        public int Scale { get; }

        public ColumnTypeAttributes(int precision, int scale)
        {
            Precision = precision;
            Scale = scale;
        }
    }
}
