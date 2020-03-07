using System.Threading.Tasks;

namespace Knet.Kudu.Client.FunctionalTests.Util
{
    public static class ClientTestUtil
    {
        public static TableBuilder GetBasicSchema()
        {
            return new TableBuilder()
                .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
                .AddColumn("column1_i", KuduType.Int32, opt => opt.Nullable(false))
                .AddColumn("column2_i", KuduType.Int32, opt => opt.Nullable(false))
                .AddColumn("column3_s", KuduType.String, opt => opt
                    .DesiredBlockSize(4096)
                    .Encoding(EncodingType.DictEncoding)
                    .Compression(CompressionType.Lz4))
                .AddColumn("column4_b", KuduType.Bool, opt => opt.Nullable(false));
        }

        public static TableBuilder CreateManyStringsSchema()
        {
            return new TableBuilder()
                .AddColumn("key", KuduType.String, opt => opt.Key(true))
                .AddColumn("c1", KuduType.String, opt => opt.Nullable(false))
                .AddColumn("c2", KuduType.String, opt => opt.Nullable(false))
                .AddColumn("c3", KuduType.String)
                .AddColumn("c4", KuduType.String);
        }

        public static TableBuilder CreateBasicRangePartition(this TableBuilder tableBuilder)
        {
            return tableBuilder.SetRangePartitionColumns("key");
        }

        public static KuduOperation CreateBasicSchemaInsert(KuduTable table, int key)
        {
            var row = table.NewInsert();
            row.SetInt32(0, key);
            row.SetInt32(1, 2);
            row.SetInt32(2, 3);
            row.SetString(3, "a string");
            row.SetBool(4, true);
            return row;
        }

        public static async Task<int> CountRowsAsync(KuduClient client, KuduTable table)
        {
            var scanner = client.NewScanBuilder(table).Build();
            var rows = 0;

            await foreach (var resultSet in scanner)
                rows += resultSet.Count;

            return rows;
        }
    }
}
