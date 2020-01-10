using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client
{
    public readonly struct ColumnBuilder
    {
        private readonly ColumnSchemaPB _column;

        public ColumnBuilder(string name, KuduType type)
        {
            _column = new ColumnSchemaPB
            {
                Name = name,
                Type = (DataTypePB)type,
                IsNullable = true
            };
        }

        /// <summary>
        /// Sets if the column is part of the row key. False by default.
        /// If the column is set as a key, it is also marked non-nullable.
        /// </summary>
        /// <param name="isKey">
        /// A bool that indicates if the column is part of the key.
        /// </param>
        public ColumnBuilder Key(bool isKey)
        {
            _column.IsKey = isKey;

            if (isKey)
                _column.IsNullable = false;

            return this;
        }

        /// <summary>
        /// Marks the column as allowing null values. True by default, unless
        /// the column is part of the row key.
        /// </summary>
        /// <param name="isNullable">
        /// A bool that indicates if the column allows null values.
        /// </param>
        public ColumnBuilder Nullable(bool isNullable)
        {
            _column.IsNullable = isNullable;
            return this;
        }

        /// <summary>
        /// Set the block encoding for this column.
        /// </summary>
        /// <param name="encodingType">The encoding to use on the column.</param>
        public ColumnBuilder Encoding(EncodingType encodingType)
        {
            _column.Encoding = (EncodingTypePB)encodingType;
            return this;
        }

        /// <summary>
        /// Set the compression algorithm for this column.
        /// </summary>
        /// <param name="compressionType">The compression to use on the column.</param>
        public ColumnBuilder Compression(CompressionType compressionType)
        {
            _column.Compression = (CompressionTypePB)compressionType;
            return this;
        }

        /// <summary>
        /// Set the desired block size for this column.
        /// 
        /// This is the number of bytes of user data packed per block on disk, and
        /// represents the unit of IO when reading this column. Larger values
        /// may improve scan performance, particularly on spinning media. Smaller
        /// values may improve random access performance, particularly for workloads
        /// that have high cache hit rates or operate on fast storage such as SSD.
        /// 
        /// Note that the block size specified here corresponds to uncompressed data.
        /// The actual size of the unit read from disk may be smaller if
        /// compression is enabled.
        /// 
        /// It's recommended that this not be set any lower than 4096 (4KB) or higher
        /// than 1048576 (1MB).
        /// </summary>
        /// <param name="desiredBlockSize">The desired block size, in bytes.</param>
        public ColumnBuilder DesiredBlockSize(int desiredBlockSize)
        {
            _column.CfileBlockSize = desiredBlockSize;
            return this;
        }

        /// <summary>
        /// Set the decimal attributes for this column.
        /// </summary>
        /// <param name="precision">The decimal precision.</param>
        /// <param name="scale">The decimal scale.</param>
        public ColumnBuilder DecimalAttributes(int precision, int scale)
        {
            _column.TypeAttributes = new ColumnTypeAttributesPB
            {
                Precision = precision,
                Scale = scale
            };

            return this;
        }

        /// <summary>
        /// Set the comment for this column.
        /// </summary>
        /// <param name="comment">The comment to set on the column.</param>
        public ColumnBuilder Comment(string comment)
        {
            _column.Comment = comment;
            return this;
        }

        public static implicit operator ColumnSchemaPB(ColumnBuilder builder) => builder._column;
    }
}
