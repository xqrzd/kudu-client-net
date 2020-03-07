using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ScanTokenTests : IAsyncLifetime
    {
        private readonly string _tableName = "TestScanToken";
        private readonly KuduTestHarness _harness;
        private readonly KuduClient _client;
        private readonly IKuduSession _session;

        public ScanTokenTests()
        {
            _harness = new MiniKuduClusterBuilder().BuildHarness();
            _client = _harness.CreateClient();
            _session = _client.NewSession();
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            await _session.DisposeAsync();
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        /// <summary>
        /// Tests scan tokens by creating a set of scan tokens, serializing them, and
        /// then executing them in parallel with separate client instances. This
        /// simulates the normal usecase of scan tokens being created at a central
        /// planner and distributed to remote task executors.
        /// </summary>
        [SkippableFact]
        public async Task TestScanTokens()
        {
            var builder = ClientTestUtil.CreateManyStringsSchema()
                .SetTableName(_tableName)
                .AddHashPartitions(8, "key")
                .CreateBasicRangePartition()
                .AddSplitRow(row => row.SetString("key", "key_50"));

            var table = await _client.CreateTableAsync(builder);

            for (int i = 0; i < 100; i++)
            {
                var row = table.NewInsert();
                row.SetString("key", $"key_{i}");
                row.SetString("c1", "c1_" + i);
                row.SetString("c2", "c2_" + i);

                await _session.EnqueueAsync(row);
            }

            await _session.FlushAsync();

            var tokenBuilder = _client.NewScanTokenBuilder(table)
                .SetEmptyProjection()
                // For this test, make sure that we cover the case that not all tablets
                // are returned in a single batch.
                .SetFetchTabletsPerRangeLookup(4);

            List<KuduScanToken> tokens = await tokenBuilder.BuildAsync();
            Assert.Equal(16, tokens.Count);

            await using var newClient = _harness.CreateClient();
            var rowCount = await CountScanTokenRowsAsync(newClient, table, tokens);

            Assert.Equal(100, rowCount);
        }

        /// <summary>
        /// Tests scan token creation and execution on a table with non-covering
        /// range partitions.
        /// </summary>
        [SkippableFact]
        public async Task TestScanTokensNonCoveringRangePartitions()
        {
            var builder = ClientTestUtil.CreateManyStringsSchema()
                .SetTableName(_tableName)
                .AddHashPartitions(2, "key")
                .CreateBasicRangePartition()
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetString("key", "a");
                    upper.SetString("key", "f");
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetString("key", "h");
                    upper.SetString("key", "z");
                })
                .AddSplitRow(row => row.SetString("key", "k"));

            var table = await _client.CreateTableAsync(builder);

            for (char c = 'a'; c < 'f'; c++)
            {
                var row = table.NewInsert();
                row.SetString("key", "" + c);
                row.SetString("c1", "c1_" + c);
                row.SetString("c2", "c2_" + c);

                await _session.EnqueueAsync(row);
            }

            for (char c = 'h'; c < 'z'; c++)
            {
                var row = table.NewInsert();
                row.SetString("key", "" + c);
                row.SetString("c1", "c1_" + c);
                row.SetString("c2", "c2_" + c);

                await _session.EnqueueAsync(row);
            }

            await _session.FlushAsync();

            var tokenBuilder = _client.NewScanTokenBuilder(table)
                .SetEmptyProjection();

            List<KuduScanToken> tokens = await tokenBuilder.BuildAsync();
            Assert.Equal(6, tokens.Count);

            await using var newClient = _harness.CreateClient();
            var rowCount = await CountScanTokenRowsAsync(newClient, table, tokens);

            Assert.Equal('f' - 'a' + 'z' - 'h', rowCount);
        }

        /// <summary>
        /// Tests the results of creating scan tokens, altering the columns being
        /// scanned, and then executing the scan tokens.
        /// </summary>
        [SkippableFact]
        public async Task TestScanTokensConcurrentAlterTable()
        {
            var builder = new TableBuilder(_tableName)
                .SetNumReplicas(1)
                .AddColumn("key", KuduType.Int64, opt => opt.Key(true))
                .AddColumn("a", KuduType.Int64);

            var table = await _client.CreateTableAsync(builder);

            List<KuduScanToken> tokens = await _client.NewScanTokenBuilder(table)
                .BuildAsync();
            var token = Assert.Single(tokens);

            // Drop a column
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .DropColumn("a"));

            table = await _client.OpenTableAsync(_tableName);

            Assert.Throws<KeyNotFoundException>(() =>
            {
                _client.NewScanBuilder(table).ApplyScanToken(token);
            });

            // Add a column with the same name, type, and nullability. It will have a
            // different id-- it's a  different column-- so the scan token will fail.
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .AddColumn("a", KuduType.Int64, opt => opt.DefaultValue(0L)));

            table = await _client.OpenTableAsync(_tableName);

            Assert.Throws<KeyNotFoundException>(() =>
            {
                _client.NewScanBuilder(table).ApplyScanToken(token);
            });
        }

        /// <summary>
        /// Tests scan token creation and execution on a table with interleaved
        /// range partition drops.
        /// </summary>
        [SkippableFact]
        public async Task TestScanTokensInterleavedRangePartitionDrops()
        {
            int numRows = 30;

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .AddHashPartitions(2, "key")
                .CreateBasicRangePartition()
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 0);
                    upper.SetInt32("key", numRows / 3);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", numRows / 3);
                    upper.SetInt32("key", 2 * numRows / 3);
                })
                .AddRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 2 * numRows / 3);
                    upper.SetInt32("key", numRows);
                });

            var table = await _client.CreateTableAsync(builder);

            for (int i = 0; i < numRows; i++)
            {
                var row = ClientTestUtil.CreateBasicSchemaInsert(table, i);
                await _session.EnqueueAsync(row);
            }

            await _session.FlushAsync();

            // Build the scan tokens.
            List<KuduScanToken> tokens = await _client.NewScanTokenBuilder(table)
                .BuildAsync();
            Assert.Equal(6, tokens.Count);

            // Drop the range partition [10, 20).
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .DropRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", numRows / 3);
                    upper.SetInt32("key", 2 * numRows / 3);
                }));

            // Rehydrate the tokens.
            var scanners = new List<KuduScanner<ResultSet>>();
            foreach (var token in tokens)
            {
                var scanner = _client.NewScanBuilder(table)
                    .ApplyScanToken(token)
                    .Build();

                scanners.Add(scanner);
            }

            // Drop the range partition [20, 30).
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .DropRangePartition((lower, upper) =>
                {
                    lower.SetInt32("key", 2 * numRows / 3);
                    upper.SetInt32("key", numRows);
                }));

            // Check the scanners work. The scanners for the tablets in the range
            // [10, 20) definitely won't see any rows. The scanners for the tablets
            // in the range [20, 30) might see rows.
            int scannedRows = 0;
            foreach (var scanner in scanners)
            {
                await foreach (var resultSet in scanner)
                {
                    scannedRows += resultSet.Count;
                }
            }

            Assert.True(scannedRows >= numRows / 3);
            Assert.True(scannedRows <= 2 * numRows / 3);
        }

        /// <summary>
        /// Test that scan tokens work with diff scans.
        /// </summary>
        [SkippableFact]
        public async Task TestDiffScanTokens()
        {
            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .SetNumReplicas(1);

            var table = await _client.CreateTableAsync(builder);

            // Set up the table for a diff scan.
            int numRows = 20;
            long timestamp = await SetupTableForDiffScansAsync(table, numRows);

            // Since the diff scan interval is [start, end), increment the start timestamp
            // to exclude the last row inserted in the first group of ops, and increment the
            // end timestamp to include the last row deleted in the second group of ops.
            List<KuduScanToken> tokens = await _client.NewScanTokenBuilder(table)
                .DiffScan(timestamp + 1, _client.LastPropagatedTimestamp + 1)
                .BuildAsync();

            var token = Assert.Single(tokens);

            var scanner = _client.NewScanBuilder(table)
                .ApplyScanToken(token)
                .Build();

            await CheckDiffScanResultsAsync(scanner, 3 * numRows / 4, numRows / 4);
        }

        /// <summary>
        /// Test that scan tokens work with diff scans even when columns are renamed.
        /// </summary>
        [SkippableFact]
        public async Task TestDiffScanTokensConcurrentColumnRename()
        {
            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(_tableName)
                .SetNumReplicas(1);

            var table = await _client.CreateTableAsync(builder);

            // Set up the table for a diff scan.
            int numRows = 20;
            long timestamp = await SetupTableForDiffScansAsync(table, numRows);

            // Since the diff scan interval is [start, end), increment the start timestamp
            // to exclude the last row inserted in the first group of ops, and increment the
            // end timestamp to include the last row deleted in the second group of ops.
            List<KuduScanToken> tokens = await _client.NewScanTokenBuilder(table)
                .DiffScan(timestamp + 1, _client.LastPropagatedTimestamp + 1)
                .BuildAsync();

            var token = Assert.Single(tokens);

            // Rename a column between when the token is created and when it is
            // rehydrated into a scanner
            await _client.AlterTableAsync(new AlterTableBuilder(table)
                .RenameColumn("column1_i", "column1_i_new"));

            table = await _client.OpenTableAsync(_tableName);

            var scanner = _client.NewScanBuilder(table)
                .ApplyScanToken(token)
                .Build();

            await CheckDiffScanResultsAsync(scanner, 3 * numRows / 4, numRows / 4);
        }

        private async Task<long> SetupTableForDiffScansAsync(KuduTable table, int numRows)
        {
            //var one = _client.LastPropagatedTimestamp;
            //Console.WriteLine("1: " + one);
            for (int i = 0; i < numRows / 2; i++)
            {
                var row = ClientTestUtil.CreateBasicSchemaInsert(table, i);
                await _session.EnqueueAsync(row);
            }

            await _session.FlushAsync();

            // Grab the timestamp, then add more data so there's a diff.
            long timestamp = _client.LastPropagatedTimestamp;
            for (int i = numRows / 2; i < numRows; i++)
            {
                var row = ClientTestUtil.CreateBasicSchemaInsert(table, i);
                await _session.EnqueueAsync(row);
            }

            await _session.FlushAsync();

            // Delete some data so the is_deleted column can be tested.
            for (int i = 0; i < numRows / 4; i++)
            {
                var row = table.NewDelete();
                row.SetInt32(0, i);
                await _session.EnqueueAsync(row);
            }

            await _session.FlushAsync();

            return timestamp;
        }

        private async Task CheckDiffScanResultsAsync(
            KuduScanner<ResultSet> scanner,
            int numExpectedMutations,
            int numExpectedDeletes)
        {
            int numMutations = 0;
            int numDeletes = 0;

            await foreach (var resultSet in scanner)
            {
                AccumulateResults(resultSet);
            }

            void AccumulateResults(ResultSet resultSet)
            {
                foreach (var rowResult in resultSet)
                {
                    numMutations++;

                    if (rowResult.IsDeleted)
                        numDeletes++;
                }
            }

            Assert.Equal(numExpectedMutations, numMutations);
            Assert.Equal(numExpectedDeletes, numDeletes);
        }

        private static async Task<int> CountScanTokenRowsAsync(
            KuduClient client, KuduTable table, List<KuduScanToken> tokens)
        {
            var tasks = new List<Task<int>>();

            foreach (var token in tokens)
            {
                var task = Task.Run(async () =>
                {
                    var count = 0;
                    var tokenBytes = token.Serialize();

                    var scanner = client.NewScanBuilder(table)
                        .ApplyScanToken(tokenBytes)
                        .Build();

                    await foreach (var resultSet in scanner)
                    {
                        count += resultSet.Count;
                    }

                    return count;
                });

                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks);
            var rowCount = results.Sum();

            return rowCount;
        }
    }
}
