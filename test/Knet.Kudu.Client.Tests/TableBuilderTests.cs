using Knet.Kudu.Client.Protobuf;
using Xunit;

namespace Knet.Kudu.Client.Tests;

public class TableBuilderTests
{
    [Fact]
    public void CanSetBasicProperties()
    {
        var builder = new TableBuilder()
            .SetTableName("table_name")
            .SetNumReplicas(3);

        var request = builder.Build();

        Assert.Equal("table_name", request.Name);
        Assert.Equal(3, request.NumReplicas);
    }

    [Fact]
    public void CanAddColumns()
    {
        var builder = new TableBuilder()
            .AddColumn("c1", KuduType.Int32, opt => opt.Key(true))
            .AddColumn("c2", KuduType.String, opt => opt
                .Nullable(false)
                .Encoding(EncodingType.DictEncoding)
                .Compression(CompressionType.Snappy));

        var request = builder.Build();

        Assert.Collection(request.Schema.Columns, c =>
        {
            Assert.Equal("c1", c.Name);
            Assert.Equal(DataType.Int32, c.Type);
            Assert.True(c.IsKey);
            Assert.False(c.IsNullable);
            Assert.Equal(Protobuf.EncodingType.AutoEncoding, c.Encoding);
            Assert.Equal(Protobuf.CompressionType.DefaultCompression, c.Compression);
            Assert.Null(c.TypeAttributes);
        }, c =>
        {
            Assert.Equal("c2", c.Name);
            Assert.Equal(DataType.String, c.Type);
            Assert.Equal(Protobuf.EncodingType.DictEncoding, c.Encoding);
            Assert.Equal(Protobuf.CompressionType.Snappy, c.Compression);
        });
    }

    [Fact]
    public void CanSetColumnTypeAttributes()
    {
        var builder = new TableBuilder()
            .AddColumn("dec32", KuduType.Decimal32, opt => opt
                .DecimalAttributes(4, 3));

        var request = builder.Build();

        Assert.Collection(request.Schema.Columns, c =>
        {
            Assert.Equal(4, c.TypeAttributes.Precision);
            Assert.Equal(3, c.TypeAttributes.Scale);
        });
    }

    [Fact]
    public void CanAddHashPartitions()
    {
        var builder = new TableBuilder()
            .AddHashPartitions(5, "a", "b", "c");

        var request = builder.Build();

        Assert.Collection(request.PartitionSchema.HashSchema, h =>
        {
            Assert.Equal(5, h.NumBuckets);
            Assert.Equal((uint)0, h.Seed);

            Assert.Collection(h.Columns,
                c => Assert.Equal("a", c.Name),
                c => Assert.Equal("b", c.Name),
                c => Assert.Equal("c", c.Name));
        });
    }
}
