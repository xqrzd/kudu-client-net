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
        private static readonly string _tableName = "TestKuduScanner";

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
                .SetTableName(_tableName)
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
    }
}
