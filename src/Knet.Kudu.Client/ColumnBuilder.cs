namespace Knet.Kudu.Client;

public class ColumnBuilder
{
    private readonly string _name;
    private readonly KuduType _type;
    private bool _isKey;
    private bool _isNullable;
    private object? _defaultValue;
    private int _desiredBlockSize;
    private EncodingType _encoding;
    private CompressionType _compression;
    private ColumnTypeAttributes? _typeAttributes;
    private string? _comment;

    public ColumnBuilder(string name, KuduType type)
    {
        _name = name;
        _type = type;
        _isNullable = true;
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
        _isKey = isKey;

        if (isKey)
            _isNullable = false;

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
        _isNullable = isNullable;
        return this;
    }

    /// <summary>
    /// Set the block encoding for this column.
    /// </summary>
    /// <param name="encodingType">The encoding to use on the column.</param>
    public ColumnBuilder Encoding(EncodingType encodingType)
    {
        _encoding = encodingType;
        return this;
    }

    /// <summary>
    /// Set the compression algorithm for this column.
    /// </summary>
    /// <param name="compressionType">The compression to use on the column.</param>
    public ColumnBuilder Compression(CompressionType compressionType)
    {
        _compression = compressionType;
        return this;
    }

    /// <summary>
    /// <para>
    /// Set the desired block size for this column.
    /// </para>
    ///
    /// <para>
    /// This is the number of bytes of user data packed per block on disk, and
    /// represents the unit of IO when reading this column. Larger values
    /// may improve scan performance, particularly on spinning media. Smaller
    /// values may improve random access performance, particularly for workloads
    /// that have high cache hit rates or operate on fast storage such as SSD.
    /// </para>
    ///
    /// <para>
    /// Note that the block size specified here corresponds to uncompressed data.
    /// The actual size of the unit read from disk may be smaller if
    /// compression is enabled.
    /// </para>
    ///
    /// <para>
    /// It's recommended that this not be set any lower than 4096 (4KB) or higher
    /// than 1048576 (1MB).
    /// </para>
    /// </summary>
    /// <param name="desiredBlockSize">The desired block size, in bytes.</param>
    public ColumnBuilder DesiredBlockSize(int desiredBlockSize)
    {
        _desiredBlockSize = desiredBlockSize;
        return this;
    }

    /// <summary>
    /// Set the decimal attributes for this column.
    /// </summary>
    /// <param name="precision">The decimal precision.</param>
    /// <param name="scale">The decimal scale.</param>
    public ColumnBuilder DecimalAttributes(int precision, int scale)
    {
        _typeAttributes = ColumnTypeAttributes.NewDecimalAttributes(precision, scale);
        return this;
    }

    /// <summary>
    /// Set the varchar attributes for this column.
    /// </summary>
    /// <param name="length">
    /// Max length for this column, between 1 and 65535 inclusive.
    /// </param>
    public ColumnBuilder VarcharAttributes(int length)
    {
        _typeAttributes = ColumnTypeAttributes.NewVarcharAttributes(length);
        return this;
    }

    /// <summary>
    /// Set the comment for this column.
    /// </summary>
    /// <param name="comment">The comment to set on the column.</param>
    public ColumnBuilder Comment(string comment)
    {
        _comment = comment;
        return this;
    }

    /// <summary>
    /// Sets the default value that will be read from the column. Null by default.
    /// </summary>
    /// <param name="value">
    /// A C# object representation of the default value that's read.
    /// </param>
    public ColumnBuilder DefaultValue(object value)
    {
        _defaultValue = value;
        return this;
    }

    public ColumnSchema Build()
    {
        return new ColumnSchema(
            _name,
            _type,
            _isKey,
            _isNullable,
            _defaultValue,
            _desiredBlockSize,
            _encoding,
            _compression,
            _typeAttributes,
            _comment);
    }
}
