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

                    if (insert.IsNull(3))
                        Assert.True(row.IsNull(3));
                    else
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

        [SkippableFact]
        public async Task TestDiffScan()
        {
            using var miniCluster = new MiniKuduClusterBuilder()
                .AddTabletServerFlag("--flush_threshold_secs=1")
                .Build();
            await using var client = miniCluster.CreateClient();

            var builder = new TableBuilder("TestDiffScan")
                .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
                // Include a column with the default IS_DELETED column name to test collision handling.
                .AddColumn("is_deleted", KuduType.Int32, opt => opt.Nullable(false))
                .CreateBasicRangePartition();

            var table = await client.CreateTableAsync(builder);

            // Generate some rows before the start time. Ensure there's at least one insert.
            int beforeBounds = 5;
            int numInserts = _random.Next(1, beforeBounds);
            int numUpdates = _random.Next(beforeBounds);
            int numDeletes = _random.Next(beforeBounds);
            var beforeOps = GenerateMutationOperations(table, numInserts, numUpdates, numDeletes);
            var before = await ApplyOperationsAsync(client, beforeOps);

            // Set the start timestamp after the initial mutations by getting the propagated timestamp,
            // and incrementing by 1.
            long startHT = client.LastPropagatedTimestamp + 1;

            // Generate row mutations.
            // The mutations performed here are what should be seen by the diff scan.
            int mutationBounds = 10;
            int expectedNumInserts = _random.Next(mutationBounds);
            int expectedNumUpdates = _random.Next(mutationBounds);
            int expectedNumDeletes = _random.Next(mutationBounds);
            var operations = GenerateMutationOperations(
                table, expectedNumInserts, expectedNumUpdates, expectedNumDeletes);
            var mutations = await ApplyOperationsAsync(client, operations);

            // Set the end timestamp after the test mutations by getting the propagated timestamp,
            // and incrementing by 1.
            long endHT = client.LastPropagatedTimestamp + 1;

            // Generate some rows after the end time.
            int afterBounds = 5;
            numInserts = _random.Next(afterBounds);
            numUpdates = _random.Next(afterBounds);
            numDeletes = _random.Next(afterBounds);
            var afterOps = GenerateMutationOperations(table, numInserts, numUpdates, numDeletes);
            var after = await ApplyOperationsAsync(client, afterOps);

            // Diff scan the time range.
            // Pass through the scan token API to ensure serialization of tokens works too.
            var tokens = await client.NewScanTokenBuilder(table)
                .DiffScan(startHT, endHT)
                .BuildAsync();

            var results = new List<TempRowResult>();
            foreach (var token in tokens)
            {
                var scanner = client.NewScanBuilder(table)
                    .ApplyScanToken(token)
                    .Build();

                await foreach (var resultSet in scanner)
                {
                    // TODO: Expose this data on the scanner.
                    // Verify the IS_DELETED column is appended at the end of the projection.
                    var projection = resultSet.Schema;
                    int isDeletedIndex = projection.IsDeletedIndex;
                    Assert.Equal(projection.Columns.Count - 1, isDeletedIndex);
                    // Verify the IS_DELETED column has the correct types.
                    var isDeletedCol = projection.GetColumn(isDeletedIndex);
                    Assert.Equal(KuduType.Bool, isDeletedCol.Type);
                    // Verify the IS_DELETED column is named to avoid collision.
                    Assert.Equal(projection.GetColumn(isDeletedIndex),
                        projection.GetColumn("is_deleted_"));

                    AccumulateResults(resultSet);
                }

                void AccumulateResults(ResultSet resultSet)
                {
                    foreach (var row in resultSet)
                    {
                        results.Add(new TempRowResult(row));
                    }
                }
            }

            // DELETEs won't be found in the results because the rows to which they
            // apply were also inserted within the diff scan's time range, which means
            // they will be excluded from the scan results.
            Assert.Equal(mutations.Count - expectedNumDeletes, results.Count);

            // Count the results and verify their change type.
            int resultNumInserts = 0;
            int resultNumUpdates = 0;
            int resultExtra = 0;
            foreach (var result in results)
            {
                var key = result.Key;
                var type = mutations[key];
                if (type == RowOperation.Insert)
                {
                    Assert.False(result.IsDeleted);
                    resultNumInserts++;
                }
                else if (type == RowOperation.Update)
                {
                    Assert.False(result.IsDeleted);
                    resultNumUpdates++;
                }
                else if (type == RowOperation.Delete)
                {
                    Assert.True(false, "Shouldn't see any DELETEs");
                }
                else
                {
                    // The key was not found in the mutations map. This means that we somehow managed to scan
                    // a row that was never mutated. It's an error and will trigger an assert below.
                    resultExtra++;
                }
            }

            Assert.Equal(expectedNumInserts, resultNumInserts);
            Assert.Equal(expectedNumUpdates, resultNumUpdates);
            Assert.Equal(0, resultExtra);
        }

        [SkippableFact]
        public async Task TestDiffScanIsDeleted()
        {
            using var miniCluster = new MiniKuduClusterBuilder().Build();
            await using var client = miniCluster.CreateClient();

            var builder = new TableBuilder("TestDiffScanIsDeleted")
                .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
                .CreateBasicRangePartition();

            var table = await client.CreateTableAsync(builder);

            // Test a very simple diff scan that should capture one deleted row.
            var insert = table.NewInsert();
            insert.SetInt32(0, 0);
            await client.WriteAsync(new[] { insert });
            long startHT = client.LastPropagatedTimestamp + 1;

            var delete = table.NewDelete();
            delete.SetInt32(0, 0);
            await client.WriteAsync(new[] { delete });
            long endHT = client.LastPropagatedTimestamp + 1;

            var scanner = client.NewScanBuilder(table)
                .DiffScan(startHT, endHT)
                .Build();

            int rowCount = 0;
            await foreach (var resultSet in scanner)
            {
                rowCount += resultSet.Count;
                CheckResults(resultSet);
            }

            Assert.Equal(1, rowCount);

            static void CheckResults(ResultSet resultSet)
            {
                foreach (var row in resultSet)
                {
                    Assert.Equal(0, row.GetInt32(0));
                    Assert.True(row.HasIsDeleted);
                    Assert.True(row.IsDeleted);
                }
            }
        }

        /// <summary>
        /// Generates a list of random mutation operations. Any unique row, identified by
        /// it's key, could have a random number of operations/mutations. However, the
        /// target count of numInserts, numUpdates and numDeletes will always be achieved
        /// if the entire list of operations is processed.
        /// </summary>
        /// <param name="table">The table to generate operations for.</param>
        /// <param name="numInserts">The number of row mutations to end with an insert.</param>
        /// <param name="numUpdates">The number of row mutations to end with an update.</param>
        /// <param name="numDeletes">The number of row mutations to end with an delete.</param>
        private List<KuduOperation> GenerateMutationOperations(
            KuduTable table, int numInserts, int numUpdates, int numDeletes)
        {
            var results = new List<KuduOperation>();
            var unfinished = new List<MutationState>();
            int minMutationsBound = 5;

            // Generate Operations to initialize all of the row with inserts.
            var changeCounts = new List<(RowOperation type, int count)>
            {
                (RowOperation.Insert, numInserts),
                (RowOperation.Update, numUpdates),
                (RowOperation.Delete, numDeletes)
            };

            foreach (var (type, count) in changeCounts)
            {
                for (int i = 0; i < count; i++)
                {
                    // Generate a random insert.
                    var insert = table.NewInsert();
                    _generator.RandomizeRow(insert);
                    var key = insert.GetInt32(0);
                    // Add the insert to the results.
                    results.Add(insert);
                    // Initialize the unfinished MutationState.
                    unfinished.Add(new MutationState(key, type, _random.Next(minMutationsBound)));
                }
            }

            // Randomly pull from the unfinished list, mutate it and add that operation to
            // the results. If it has been mutated at least the minimum number of times,
            // remove it from the unfinished list.
            while (unfinished.Count > 0)
            {
                // Get a random row to mutate.
                int index = _random.Next(unfinished.Count);
                MutationState state = unfinished[index];

                // If the row is done, remove it from unfinished and continue.
                if (state.NumMutations >= state.MinMutations && state.CurrentType == state.EndType)
                {
                    unfinished.RemoveAt(index);
                    continue;
                }

                // Otherwise, generate an operation to mutate the row based on its current ChangeType.
                //    insert -> update|delete
                //    update -> update|delete
                //    delete -> insert
                KuduOperation op;
                if (state.CurrentType == RowOperation.Insert || state.CurrentType == RowOperation.Update)
                {
                    op = _random.NextBool() ? table.NewUpdate() : table.NewDelete();
                }
                else
                {
                    // Must be a delete, so we need an insert next.
                    op = table.NewInsert();
                }

                op.SetInt32(0, state.Key);

                if (op.Operation != RowOperation.Delete)
                    _generator.RandomizeRow(op, randomizeKeys: false);

                results.Add(op);

                state.CurrentType = op.Operation;
                state.NumMutations++;
            }

            return results;
        }

        /// <summary>
        /// Applies a list of operations and returns the final change type for each key.
        /// </summary>
        private async Task<Dictionary<int, RowOperation>> ApplyOperationsAsync(
            KuduClient client, List<KuduOperation> operations)
        {
            var results = new Dictionary<int, RowOperation>();

            // If there are no operations, return early.
            if (operations.Count == 0)
                return results;

            // On some runs, wait long enough to flush at the start.
            if (_random.NextBool())
                await Task.Delay(2000);

            // Pick an int as a flush indicator so we flush once on average while applying operations.
            int flushInt = _random.Next(operations.Count);

            foreach (var operation in operations)
            {
                // On some runs, wait long enough to flush while applying operations.
                if (_random.Next(operations.Count) == flushInt)
                    await Task.Delay(2000);

                //await session.EnqueueAsync(operation);
                //await session.FlushAsync();
                await client.WriteAsync(new[] { operation });

                results[operation.GetInt32(0)] = operation.Operation;
            }

            return results;
        }

        private class MutationState
        {
            public int Key { get; }
            public RowOperation EndType { get; }
            public int MinMutations { get; }

            public RowOperation CurrentType { get; set; } = RowOperation.Insert;
            public int NumMutations { get; set; } = 0;

            public MutationState(int key, RowOperation endType, int minMutations)
            {
                Key = key;
                EndType = endType;
                MinMutations = minMutations;
            }
        }

        private class TempRowResult
        {
            public int Key { get; }

            public bool IsDeleted { get; }

            public TempRowResult(RowResult row)
            {
                Key = row.GetInt32(0);
                IsDeleted = row.IsDeleted;
            }
        }
    }
}
