using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ScannerTests
    {
        private readonly Random _random;
        private readonly DataGenerator _generator;

        public ScannerTests()
        {
            _random = new Random();
            _generator = new DataGeneratorBuilder()
                .Random(_random)
                .Build();
        }

        [SkippableFact]
        public async Task TestIterable()
        {
            using var miniCluster = new MiniKuduClusterBuilder().Build();
            await using var client = miniCluster.CreateClient();
            await using var session = client.NewSession();

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName("TestIterable")
                .CreateBasicRangePartition();

            var table = await client.CreateTableAsync(builder);

            IDictionary<int, PartialRow> inserts = new Dictionary<int, PartialRow>();
            int numRows = 10;
            for (int i = 0; i < numRows; i++)
            {
                var insert = table.NewInsert();
                _generator.RandomizeRow(insert);
                inserts.TryAdd(insert.GetInt32(0), insert);
                await session.EnqueueAsync(insert);
            }

            await session.FlushAsync();

            var scanner = client.NewScanBuilder(table).Build();

            await foreach (var resultSet in scanner)
            {
                CheckResults(resultSet);
            }

            void CheckResults(ResultSet resultSet)
            {
                foreach (var row in resultSet)
                {
                    var key = row.GetInt32(0);
                    var insert = Assert.Contains(key, inserts);

                    Assert.Equal(insert.GetInt32(1), row.GetInt32(1));
                    Assert.Equal(insert.GetInt32(2), row.GetInt32(2));
                    Assert.Equal(insert.GetString(3), row.GetString(3));
                    Assert.Equal(insert.GetBool(4), row.GetBool(4));

                    inserts.Remove(key);
                }
            }

            Assert.Empty(inserts);
        }

        // TODO: Test keep alive

        [SkippableFact]
        public async Task TestOpenScanWithDroppedPartition()
        {
            using var miniCluster = new MiniKuduClusterBuilder().Build();
            await using var client = miniCluster.CreateClient();
            await using var session = client.NewSession();

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName("TestOpenScanWithDroppedPartition")
                .CreateBasicRangePartition()
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 0);
                    upper.SetInt32("key", 1000);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 1000);
                    upper.SetInt32("key", 2000);
                });

            var table = await client.CreateTableAsync(builder);

            // Load rows into both partitions.
            int numRows = 1999;
            await ClientTestUtil.LoadDefaultTableAsync(client, table, numRows);

            // Scan the rows while dropping a partition.
            var scanner = client.NewScanBuilder(table)
                // Set a small batch size so the first scan doesn't read all the rows.
                .SetBatchSizeBytes(100)
                .Build();

            int rowsScanned = 0;
            int batchNum = 0;

            await foreach (var resultSet in scanner)
            {
                if (batchNum == 1)
                {
                    // Drop the partition.
                    await client.AlterTableAsync(new AlterTableBuilder(table)
                        .DropRangePartition((lower, upper) =>
                        {
                            lower.SetInt32("key", 0);
                            upper.SetInt32("key", 1000);
                        }));

                    // Give time for the background drop operations.
                    await Task.Delay(1000);

                    // TODO: Verify the partition was dropped.
                }

                rowsScanned += resultSet.Count;
                batchNum++;
            }

            Assert.True(batchNum > 1);
            Assert.Equal(numRows, rowsScanned);
        }
    }
}
