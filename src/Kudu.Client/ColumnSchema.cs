using System;
using Kudu.Client.Builder;
using Kudu.Client.Protocol;

namespace Kudu.Client
{
    public class ColumnSchema : IEquatable<ColumnSchema>
    {
        public string Name { get; }

        public DataType Type { get; }

        public bool IsKey { get; }

        public bool IsNullable { get; }

        public EncodingType Encoding { get; }

        public CompressionType Compression { get; }

        public ColumnTypeAttributes TypeAttributes { get; }

        public int Size { get; }

        public bool IsSigned { get; }

        public ColumnSchema(string name, DataType type,
            bool isKey, bool isNullable,
            EncodingType encoding, CompressionType compression,
            ColumnTypeAttributes typeAttributes)
        {
            Name = name;
            Type = type;
            IsKey = isKey;
            IsNullable = isNullable;
            Encoding = encoding;
            Compression = compression;
            TypeAttributes = typeAttributes;

            Size = Schema.GetTypeSize(Type);
            IsSigned = Schema.IsSigned(Type);
        }

        // TODO: Move this to extension method?
        public ColumnSchema(ColumnSchemaPB columnSchemaPb)
        {
            Name = columnSchemaPb.Name;
            Type = (DataType)columnSchemaPb.Type;
            IsKey = columnSchemaPb.IsKey;
            IsNullable = columnSchemaPb.IsNullable;
            Encoding = (EncodingType)columnSchemaPb.Encoding;
            Compression = (CompressionType)columnSchemaPb.Compression;

            if (columnSchemaPb.TypeAttributes != null)
            {
                TypeAttributes = new ColumnTypeAttributes(
                    columnSchemaPb.TypeAttributes.Precision,
                    columnSchemaPb.TypeAttributes.Scale);
            }

            Size = Schema.GetTypeSize(Type);
            IsSigned = Schema.IsSigned(Type);
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
