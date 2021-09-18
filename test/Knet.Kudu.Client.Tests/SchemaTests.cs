using Xunit;

namespace Knet.Kudu.Client.Tests;

public class SchemaTests
{
    [Fact]
    public void SingleInt8()
    {
        var builder = new TableBuilder()
            .AddColumn("int8", KuduType.Int8, opt => opt.Key(true));

        var schema = GetSchema(builder);

        Assert.Equal(1, schema.Columns.Count);
        Assert.False(schema.HasNullableColumns);
        Assert.Equal(0, schema.VarLengthColumnCount);
        Assert.Equal(1, schema.RowAllocSize);

        // First column
        Assert.Equal(0, schema.GetColumnIndex(name: "int8"));
        Assert.Equal(0, schema.GetColumnIndex(id: 0));
        Assert.Equal(0, schema.GetColumnOffset(index: 0));
        var column1 = schema.GetColumn(0);
        Assert.Equal("int8", column1.Name);
        Assert.Equal(KuduType.Int8, column1.Type);
        Assert.True(column1.IsKey);
        Assert.False(column1.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column1.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column1.Compression);
        Assert.Null(column1.TypeAttributes);
        Assert.Equal(1, column1.Size);
        Assert.True(column1.IsSigned);
    }

    [Fact]
    public void MultipleStrings()
    {
        var builder = new TableBuilder()
            .AddColumn("string1", KuduType.String, opt => opt.Key(true))
            .AddColumn("string2", KuduType.String);

        var schema = GetSchema(builder);

        Assert.Equal(2, schema.Columns.Count);
        Assert.True(schema.HasNullableColumns);
        Assert.Equal(2, schema.VarLengthColumnCount);
        Assert.Equal(0, schema.RowAllocSize);

        // First column
        Assert.Equal(0, schema.GetColumnIndex(name: "string1"));
        Assert.Equal(0, schema.GetColumnIndex(id: 0));
        Assert.Equal(0, schema.GetColumnOffset(index: 0));
        var column1 = schema.GetColumn(0);
        Assert.Equal("string1", column1.Name);
        Assert.Equal(KuduType.String, column1.Type);
        Assert.True(column1.IsKey);
        Assert.False(column1.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column1.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column1.Compression);
        Assert.Null(column1.TypeAttributes);
        Assert.Equal(16, column1.Size);
        Assert.False(column1.IsSigned);

        // Second column
        Assert.Equal(1, schema.GetColumnIndex(name: "string2"));
        Assert.Equal(1, schema.GetColumnIndex(id: 1));
        Assert.Equal(1, schema.GetColumnOffset(index: 1));
        var column2 = schema.GetColumn(1);
        Assert.Equal("string2", column2.Name);
        Assert.Equal(KuduType.String, column2.Type);
        Assert.False(column2.IsKey);
        Assert.True(column2.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column2.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column2.Compression);
        Assert.Null(column2.TypeAttributes);
        Assert.Equal(16, column2.Size);
        Assert.False(column2.IsSigned);
    }

    [Fact]
    public void MultipleTypes()
    {
        var builder = new TableBuilder()
            .AddColumn("string", KuduType.String, opt => opt.Key(true))
            .AddColumn("int32", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("bool", KuduType.Bool, opt => opt.Nullable(false))
            .AddColumn("bin", KuduType.Binary, opt => opt.Nullable(false))
            .AddColumn("decimal64", KuduType.Decimal64, opt => opt
                .Nullable(false)
                .DecimalAttributes(12, 4))
            .AddColumn("timestamp", KuduType.UnixtimeMicros, opt => opt.Nullable(false))
            .AddColumn("double", KuduType.Double, opt => opt.Nullable(false));

        var schema = GetSchema(builder);

        Assert.Equal(7, schema.Columns.Count);
        Assert.False(schema.HasNullableColumns);
        Assert.Equal(2, schema.VarLengthColumnCount);
        Assert.Equal(29, schema.RowAllocSize);

        Assert.Equal(0, schema.GetColumnIndex(name: "string"));
        Assert.Equal(0, schema.GetColumnIndex(id: 0));
        Assert.Equal(0, schema.GetColumnOffset(index: 0));
        var column1 = schema.GetColumn(0);
        Assert.Equal("string", column1.Name);
        Assert.Equal(KuduType.String, column1.Type);
        Assert.True(column1.IsKey);
        Assert.False(column1.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column1.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column1.Compression);
        Assert.Null(column1.TypeAttributes);
        Assert.Equal(16, column1.Size);
        Assert.False(column1.IsSigned);

        Assert.Equal(1, schema.GetColumnIndex(name: "int32"));
        Assert.Equal(1, schema.GetColumnIndex(id: 1));
        Assert.Equal(0, schema.GetColumnOffset(index: 1));
        var column2 = schema.GetColumn(1);
        Assert.Equal("int32", column2.Name);
        Assert.Equal(KuduType.Int32, column2.Type);
        Assert.True(column2.IsKey);
        Assert.False(column2.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column2.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column2.Compression);
        Assert.Null(column2.TypeAttributes);
        Assert.Equal(4, column2.Size);
        Assert.True(column2.IsSigned);

        Assert.Equal(2, schema.GetColumnIndex(name: "bool"));
        Assert.Equal(2, schema.GetColumnIndex(id: 2));
        Assert.Equal(4, schema.GetColumnOffset(index: 2));
        var column3 = schema.GetColumn(2);
        Assert.Equal("bool", column3.Name);
        Assert.Equal(KuduType.Bool, column3.Type);
        Assert.False(column3.IsKey);
        Assert.False(column3.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column3.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column3.Compression);
        Assert.Null(column3.TypeAttributes);
        Assert.Equal(1, column3.Size);
        Assert.False(column3.IsSigned);

        Assert.Equal(3, schema.GetColumnIndex(name: "bin"));
        Assert.Equal(3, schema.GetColumnIndex(id: 3));
        Assert.Equal(1, schema.GetColumnOffset(index: 3));
        var column4 = schema.GetColumn(3);
        Assert.Equal("bin", column4.Name);
        Assert.Equal(KuduType.Binary, column4.Type);
        Assert.False(column4.IsKey);
        Assert.False(column4.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column4.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column4.Compression);
        Assert.Null(column4.TypeAttributes);
        Assert.Equal(16, column4.Size);
        Assert.False(column4.IsSigned);

        Assert.Equal(4, schema.GetColumnIndex(name: "decimal64"));
        Assert.Equal(4, schema.GetColumnIndex(id: 4));
        Assert.Equal(5, schema.GetColumnOffset(index: 4));
        var column5 = schema.GetColumn(4);
        Assert.Equal("decimal64", column5.Name);
        Assert.Equal(KuduType.Decimal64, column5.Type);
        Assert.False(column5.IsKey);
        Assert.False(column5.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column5.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column5.Compression);
        Assert.Equal(12, column5.TypeAttributes.Precision);
        Assert.Equal(4, column5.TypeAttributes.Scale);
        Assert.Equal(8, column5.Size);
        Assert.True(column5.IsSigned);

        Assert.Equal(5, schema.GetColumnIndex(name: "timestamp"));
        Assert.Equal(5, schema.GetColumnIndex(id: 5));
        Assert.Equal(13, schema.GetColumnOffset(index: 5));
        var column6 = schema.GetColumn(5);
        Assert.Equal("timestamp", column6.Name);
        Assert.Equal(KuduType.UnixtimeMicros, column6.Type);
        Assert.False(column6.IsKey);
        Assert.False(column6.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column6.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column6.Compression);
        Assert.Null(column6.TypeAttributes);
        Assert.Equal(8, column6.Size);
        Assert.True(column6.IsSigned);

        Assert.Equal(6, schema.GetColumnIndex(name: "double"));
        Assert.Equal(6, schema.GetColumnIndex(id: 6));
        Assert.Equal(21, schema.GetColumnOffset(index: 6));
        var column7 = schema.GetColumn(6);
        Assert.Equal("double", column7.Name);
        Assert.Equal(KuduType.Double, column7.Type);
        Assert.False(column7.IsKey);
        Assert.False(column7.IsNullable);
        Assert.Equal(EncodingType.AutoEncoding, column7.Encoding);
        Assert.Equal(CompressionType.DefaultCompression, column7.Compression);
        Assert.Null(column7.TypeAttributes);
        Assert.Equal(8, column7.Size);
        Assert.False(column7.IsSigned);
    }

    private static KuduSchema GetSchema(TableBuilder builder)
    {
        var request = builder.Build();
        // The builder doesn't support setting column ids, so for testing
        // set them here, and use the column index as the id.
        for (var i = 0; i < request.Schema.Columns.Count; i++)
        {
            request.Schema.Columns[i].Id = (uint)i;
        }
        return new KuduSchema(request.Schema);
    }
}
