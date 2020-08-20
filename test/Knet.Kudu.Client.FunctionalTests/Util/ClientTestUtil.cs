using System;
using System.Collections.Generic;
using System.Linq;
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

        public static TableBuilder CreateAllTypesSchema(bool nullable = true)
        {
            return new TableBuilder()
                .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
                .AddColumn("int8", KuduType.Int8, opt => opt.Nullable(nullable))
                .AddColumn("int16", KuduType.Int16, opt => opt.Nullable(nullable))
                .AddColumn("int32", KuduType.Int32, opt => opt.Nullable(nullable))
                .AddColumn("int64", KuduType.Int64, opt => opt.Nullable(nullable))
                .AddColumn("bool", KuduType.Bool, opt => opt.Nullable(nullable))
                .AddColumn("float", KuduType.Float, opt => opt.Nullable(nullable))
                .AddColumn("double", KuduType.Double, opt => opt.Nullable(nullable))
                .AddColumn("string", KuduType.String, opt => opt.Nullable(nullable))
                .AddColumn("varchar", KuduType.Varchar, opt => opt.Nullable(nullable).VarcharAttributes(10))
                .AddColumn("binary", KuduType.Binary, opt => opt.Nullable(nullable))
                .AddColumn("timestamp", KuduType.UnixtimeMicros, opt => opt.Nullable(nullable))
                .AddColumn("date", KuduType.Date, opt => opt.Nullable(nullable))
                .AddColumn("decimal32", KuduType.Decimal32, opt => opt.Nullable(nullable).DecimalAttributes(5, 3))
                .AddColumn("decimal64", KuduType.Decimal64, opt => opt.Nullable(nullable).DecimalAttributes(5, 3))
                .AddColumn("decimal128", KuduType.Decimal128, opt => opt.Nullable(nullable).DecimalAttributes(5, 3));
        }

        public static TableBuilder CreateBasicRangePartition(this TableBuilder tableBuilder)
        {
            return tableBuilder.SetRangePartitionColumns("key");
        }

        /// <summary>
        /// Adds non-covering range partitioning for a table with the basic schema.
        /// Range partition key ranges fall between the following values:
        /// 
        /// [  0,  50)
        /// [ 50, 100)
        /// [200, 300)
        /// </summary>
        public static TableBuilder CreateBasicNonCoveredRangePartitions(this TableBuilder tableBuilder)
        {
            return tableBuilder
                .SetRangePartitionColumns("key")
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 0);
                    upper.SetInt32("key", 100);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 200);
                    upper.SetInt32("key", 300);
                })
                .AddSplitRow(row => row.SetInt32("key", 50));
        }

        /// <summary>
        /// Load a table of default schema with the specified number of records, in ascending key order.
        /// </summary>
        public static async Task LoadDefaultTableAsync(KuduClient client, KuduTable table, int numRows)
        {
            var rows = Enumerable.Range(0, numRows)
                .Select(i => CreateBasicSchemaInsert(table, i));

            await client.WriteAsync(rows);
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

        public static KuduOperation CreateBasicSchemaInsertIgnore(KuduTable table, int key)
        {
            var row = table.NewInsertIgnore();
            row.SetInt32(0, key);
            row.SetInt32(1, 2);
            row.SetInt32(2, 3);
            row.SetString(3, "a string");
            row.SetBool(4, true);
            return row;
        }

        public static KuduOperation CreateBasicSchemaUpsert(
            KuduTable table, int key, int secondVal, bool hasNull)
        {
            var row = table.NewUpsert();
            row.SetInt32(0, key);
            row.SetInt32(1, secondVal);
            row.SetInt32(2, 3);
            if (hasNull)
                row.SetNull(3);
            else
                row.SetString(3, "a string");
            row.SetBool(4, true);
            return row;
        }

        public static KuduOperation CreateAllTypesInsert(KuduTable table, int key)
        {
            var row = table.NewUpsert();
            row.SetInt32("key", key);
            row.SetByte("int8", 42);
            row.SetInt16("int16", 43);
            row.SetInt32("int32", 44);
            row.SetInt64("int64", 45);
            row.SetBool("bool", true);
            row.SetFloat("float", 52.35f);
            row.SetDouble("double", 53.35);
            row.SetString("string", "fun with ütf\0");
            row.SetString("varchar", "árvíztűrő tükörfúrógép");
            row.SetBinary("binary", new byte[] { 0, 1, 2, 3, 4 });
            row.SetDateTime("timestamp", DateTime.Parse("8/19/2020 7:50 PM"));
            row.SetDateTime("date", DateTime.Parse("8/19/2020"));
            row.SetDecimal("decimal32", 12.345m);
            row.SetDecimal("decimal64", 12.346m);
            row.SetDecimal("decimal128", 12.347m);
            return row;
        }

        public static KuduOperation CreateAllNullsInsert(KuduTable table, int key)
        {
            var numColumns = table.Schema.Columns.Count;
            var row = table.NewUpsert();
            row.SetInt32(0, key);
            for (int i = 1; i < numColumns; i++)
            {
                row.SetNull(i);
            }
            return row;
        }

        public static Task<int> CountRowsAsync(KuduClient client, KuduTable table)
        {
            var scanner = client.NewScanBuilder(table).Build();
            return CountRowsInScanAsync(scanner);
        }

        public static async Task<int> CountRowsInScanAsync(KuduScanner<ResultSet> scanner)
        {
            var rows = 0;

            await foreach (var resultSet in scanner)
                rows += resultSet.Count;

            return rows;
        }

        public static async Task<int> CountRowsInScanAsync(KuduScanEnumerator<ResultSet> scanner)
        {
            var rows = 0;

            while (await scanner.MoveNextAsync())
                rows += scanner.Current.Count;

            return rows;
        }

        public static async Task<List<string>> ScanTableToStringsAsync(
            KuduClient client, KuduTable table, params KuduPredicate[] predicates)
        {
            var rowStrings = new List<string>();
            var scanBuilder = client.NewScanBuilder(table);

            foreach (var predicate in predicates)
                scanBuilder.AddPredicate(predicate);

            var scanner = scanBuilder.Build();

            await foreach (var resultSet in scanner)
            {
                ParseResults(resultSet);
            }

            void ParseResults(ResultSet resultSet)
            {
                foreach (var row in resultSet)
                {
                    rowStrings.Add(row.ToString());
                }
            }

            rowStrings.Sort();

            return rowStrings;
        }
    }
}
