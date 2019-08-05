using Kudu.Client.Builder;
using Kudu.Client.Protocol;
using Kudu.Client.Protocol.Master;
using Xunit;

namespace Kudu.Client.Tests
{
    public class TableBuilderTests
    {
        [Fact]
        public void CanSetBasicProperties()
        {
            var builder = new TableBuilder()
                .SetTableName("table_name")
                .SetNumReplicas(3);

            CreateTableRequestPB request = builder;

            Assert.Equal("table_name", request.Name);
            Assert.Equal(3, request.NumReplicas);
        }

        [Fact]
        public void CanAddColumns()
        {
            var builder = new TableBuilder()
                .AddColumn(column =>
                {
                    column.Name = "c1";
                    column.Type = KuduType.Int32;
                    column.IsKey = true;
                    column.IsNullable = false;
                })
                .AddColumn(column =>
                {
                    column.Name = "c2";
                    column.Encoding = EncodingType.DictEncoding;
                    column.Compression = CompressionType.Snappy;
                });

            CreateTableRequestPB request = builder;

            Assert.Collection(request.Schema.Columns,
                c => {
                    Assert.Equal("c1", c.Name);
                    Assert.Equal(DataTypePB.Int32, c.Type);
                    Assert.True(c.IsKey);
                    Assert.False(c.IsNullable);
                    Assert.Equal(EncodingTypePB.AutoEncoding, c.Encoding);
                    Assert.Equal(CompressionTypePB.DefaultCompression, c.Compression);
                    Assert.Null(c.TypeAttributes);
                },
                c => {
                    Assert.Equal("c2", c.Name);
                    Assert.Equal(EncodingTypePB.DictEncoding, c.Encoding);
                    Assert.Equal(CompressionTypePB.Snappy, c.Compression);
                });
        }

        [Fact]
        public void CanSetColumnTypeAttributes()
        {
            var builder = new TableBuilder()
                .AddColumn(column =>
                {
                    column.Precision = 4;
                    column.Scale = 3;
                });

            CreateTableRequestPB request = builder;

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

            CreateTableRequestPB request = builder;

            Assert.Collection(request.PartitionSchema.HashBucketSchemas, h =>
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
}
