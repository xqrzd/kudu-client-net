using Kudu.Client.Builder;
using Kudu.Client.Protocol.Master;
using Xunit;

namespace Kudu.Client.Tests
{
    public class SchemaTests
    {
        [Fact]
        public void SingleInt8()
        {
            var builder = new TableBuilder()
                .AddColumn(c =>
                {
                    c.Name = "int8";
                    c.Type = DataType.Int8;
                    c.IsKey = true;
                });

            var schema = GetSchema(builder);

            Assert.Equal(1, schema.ColumnCount);
            Assert.False(schema.HasNullableColumns);
            Assert.Equal(0, schema.VarLengthColumnCount);
            Assert.Equal(1, schema.RowAllocSize);

            // First column
            Assert.Equal(0, schema.GetColumnIndex(name: "int8"));
            Assert.Equal(0, schema.GetColumnIndex(id: 0));
            Assert.True(schema.IsPrimaryKey(index: 0));
            Assert.Equal(DataType.Int8, schema.GetColumnType(index: 0));
            Assert.Equal(1, schema.GetColumnSize(index: 0));
            Assert.Equal(0, schema.GetColumnOffset(index: 0));
        }

        [Fact]
        public void MultipleStrings()
        {
            var builder = new TableBuilder()
                .AddColumn(c =>
                {
                    c.Name = "string1";
                    c.Type = DataType.String;
                    c.IsKey = true;
                })
                .AddColumn(c =>
                {
                    c.Name = "string2";
                    c.Type = DataType.String;
                    c.IsNullable = true;
                });

            var schema = GetSchema(builder);

            Assert.Equal(2, schema.ColumnCount);
            Assert.True(schema.HasNullableColumns);
            Assert.Equal(2, schema.VarLengthColumnCount);
            Assert.Equal(0, schema.RowAllocSize);

            // First column
            Assert.Equal(0, schema.GetColumnIndex(name: "string1"));
            Assert.Equal(0, schema.GetColumnIndex(id: 0));
            Assert.True(schema.IsPrimaryKey(index: 0));
            Assert.Equal(DataType.String, schema.GetColumnType(index: 0));
            Assert.Equal(16, schema.GetColumnSize(index: 0));
            Assert.Equal(0, schema.GetColumnOffset(index: 0));

            // Second column
            Assert.Equal(1, schema.GetColumnIndex(name: "string2"));
            Assert.Equal(1, schema.GetColumnIndex(id: 1));
            Assert.False(schema.IsPrimaryKey(index: 1));
            Assert.Equal(DataType.String, schema.GetColumnType(index: 1));
            Assert.Equal(16, schema.GetColumnSize(index: 1));
            Assert.Equal(1, schema.GetColumnOffset(index: 1));
        }

        private Schema GetSchema(TableBuilder builder)
        {
            CreateTableRequestPB request = builder;
            // The builder doesn't support setting column ids, so for testing
            // set them here, and use the column index as the id.
            for (var i = 0; i < request.Schema.Columns.Count; i++)
            {
                request.Schema.Columns[i].Id = (uint)i;
            }
            return new Schema(request.Schema);
        }
    }
}
