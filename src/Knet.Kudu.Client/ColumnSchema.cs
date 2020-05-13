﻿using System;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Util;

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
            IsFixedSize = IsTypeFixedSize(type);
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
            var typeAttributes = TypeAttributes?.ToStringForType(Type);
            var nullable = IsNullable ? "?" : "";
            var key = IsKey ? " PK" : "";

            return $"{Name} {Type}{typeAttributes}{nullable}{key}";
        }

        public static bool IsTypeFixedSize(KuduType type)
        {
            return
                type != KuduType.String &&
                type != KuduType.Binary &&
                type != KuduType.Varchar;
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
                columnSchemaPb.TypeAttributes.ToTypeAttributes());
        }
    }

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
    }
}
