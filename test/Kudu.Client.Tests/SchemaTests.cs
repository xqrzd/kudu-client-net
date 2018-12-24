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

        [Fact]
        public void MultipleTypes()
        {
            var builder = new TableBuilder()
                .AddColumn(c =>
                {
                    c.Name = "string";
                    c.Type = DataType.String;
                    c.IsKey = true;
                })
                .AddColumn(c =>
                {
                    c.Name = "uint32";
                    c.Type = DataType.UInt32;
                    c.IsKey = true;
                })
                .AddColumn(c =>
                {
                    c.Name = "bool";
                    c.Type = DataType.Bool;
                })
                .AddColumn(c =>
                {
                    c.Name = "bin";
                    c.Type = DataType.Binary;
                })
                .AddColumn(c =>
                {
                    c.Name = "decimal64";
                    c.Type = DataType.Decimal64;
                    c.Scale = 12;
                    c.Precision = 3;
                })
                .AddColumn(c =>
                {
                    c.Name = "timestamp";
                    c.Type = DataType.UnixtimeMicros;
                })
                .AddColumn(c =>
                {
                    c.Name = "double";
                    c.Type = DataType.Double;
                });

            var schema = GetSchema(builder);

            Assert.Equal(7, schema.ColumnCount);
            Assert.False(schema.HasNullableColumns);
            Assert.Equal(2, schema.VarLengthColumnCount);
            Assert.Equal(29, schema.RowAllocSize);

            Assert.Equal(0, schema.GetColumnIndex(name: "string"));
            Assert.Equal(0, schema.GetColumnIndex(id: 0));
            Assert.True(schema.IsPrimaryKey(index: 0));
            Assert.Equal(DataType.String, schema.GetColumnType(index: 0));
            Assert.Equal(16, schema.GetColumnSize(index: 0));
            Assert.Equal(0, schema.GetColumnOffset(index: 0));

            Assert.Equal(1, schema.GetColumnIndex(name: "uint32"));
            Assert.Equal(1, schema.GetColumnIndex(id: 1));
            Assert.True(schema.IsPrimaryKey(index: 1));
            Assert.Equal(DataType.UInt32, schema.GetColumnType(index: 1));
            Assert.Equal(4, schema.GetColumnSize(index: 1));
            Assert.Equal(0, schema.GetColumnOffset(index: 1));

            Assert.Equal(2, schema.GetColumnIndex(name: "bool"));
            Assert.Equal(2, schema.GetColumnIndex(id: 2));
            Assert.False(schema.IsPrimaryKey(index: 2));
            Assert.Equal(DataType.Bool, schema.GetColumnType(index: 2));
            Assert.Equal(1, schema.GetColumnSize(index: 2));
            Assert.Equal(4, schema.GetColumnOffset(index: 2));

            Assert.Equal(3, schema.GetColumnIndex(name: "bin"));
            Assert.Equal(3, schema.GetColumnIndex(id: 3));
            Assert.False(schema.IsPrimaryKey(index: 3));
            Assert.Equal(DataType.Binary, schema.GetColumnType(index: 3));
            Assert.Equal(16, schema.GetColumnSize(index: 3));
            Assert.Equal(1, schema.GetColumnOffset(index: 3));

            Assert.Equal(4, schema.GetColumnIndex(name: "decimal64"));
            Assert.Equal(4, schema.GetColumnIndex(id: 4));
            Assert.False(schema.IsPrimaryKey(index: 4));
            Assert.Equal(DataType.Decimal64, schema.GetColumnType(index: 4));
            Assert.Equal(8, schema.GetColumnSize(index: 4));
            Assert.Equal(5, schema.GetColumnOffset(index: 4));

            Assert.Equal(5, schema.GetColumnIndex(name: "timestamp"));
            Assert.Equal(5, schema.GetColumnIndex(id: 5));
            Assert.False(schema.IsPrimaryKey(index: 5));
            Assert.Equal(DataType.UnixtimeMicros, schema.GetColumnType(index: 5));
            Assert.Equal(8, schema.GetColumnSize(index: 5));
            Assert.Equal(13, schema.GetColumnOffset(index: 5));

            Assert.Equal(6, schema.GetColumnIndex(name: "double"));
            Assert.Equal(6, schema.GetColumnIndex(id: 6));
            Assert.False(schema.IsPrimaryKey(index: 6));
            Assert.Equal(DataType.Double, schema.GetColumnType(index: 6));
            Assert.Equal(8, schema.GetColumnSize(index: 6));
            Assert.Equal(21, schema.GetColumnOffset(index: 6));
        }

        private static Schema GetSchema(TableBuilder builder)
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
