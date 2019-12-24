using Knet.Kudu.Client.Builder;

namespace Knet.Kudu.Client.FunctionalTests.Util
{
    public static class ClientTestUtil
    {
        public static TableBuilder GetBasicSchema()
        {
            var builder = new TableBuilder()
                .AddColumn(c =>
                {
                    c.Name = "key";
                    c.Type = KuduType.Int32;
                    c.IsKey = true;
                })
                .AddColumn(c =>
                {
                    c.Name = "column1_i";
                    c.Type = KuduType.Int32;
                })
                .AddColumn(c =>
                {
                    c.Name = "column2_i";
                    c.Type = KuduType.Int32;
                })
                .AddColumn(c =>
                {
                    c.Name = "column3_s";
                    c.Type = KuduType.String;
                    c.IsNullable = true;
                    c.CfileBlockSize = 4096;
                    c.Encoding = EncodingType.DictEncoding;
                    c.Compression = CompressionType.Lz4;
                })
                .AddColumn(c =>
                {
                    c.Name = "column4_b";
                    c.Type = KuduType.Bool;
                });

            return builder;
        }

        public static TableBuilder CreateBasicRangePartition(this TableBuilder tableBuilder)
        {
            return tableBuilder.SetRangePartitionColumns("key");
        }

        public static Operation CreateBasicSchemaInsert(KuduTable table, int key)
        {
            var insert = table.NewInsert();
            var row = insert.Row;
            row.SetInt32(0, key);
            row.SetInt32(1, 2);
            row.SetInt32(2, 3);
            row.SetString(3, "a string");
            row.SetBool(4, true);
            return insert;
        }
    }
}
