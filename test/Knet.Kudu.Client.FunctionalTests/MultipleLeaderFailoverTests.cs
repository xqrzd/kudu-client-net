using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class MultipleLeaderFailoverTests
    {
        /// <summary>
        /// This test writes 3 rows. Then in a loop, it kills the leader, then
        /// tries to write inner_row rows, and finally restarts the tablet server
        /// it killed. Verifying with a read as it goes. Finally it counts to make
        /// sure we have total_rows_to_insert of them.
        /// </summary>
        [SkippableTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestMultipleFailover(bool restart)
        {
            int rowsPerIteration = 3;
            int numIterations = 10;
            int totalRowsToInsert = rowsPerIteration + numIterations * rowsPerIteration;

            await using var harness = new MiniKuduClusterBuilder()
                .NumMasters(3)
                .NumTservers(3)
                .BuildHarness();

            await using var client = harness.CreateClient();

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName("MultipleLeaderFailoverTest");

            var table = await client.CreateTableAsync(builder);
            await using var session = client.NewSession();

            for (int i = 0; i < rowsPerIteration; i++)
            {
                var row = ClientTestUtil.CreateBasicSchemaInsert(table, i);
                await session.EnqueueAsync(row);
            }

            await session.FlushAsync();

            var rowCount = await ClientTestUtil.CountRowsAsync(client, table);
            Assert.Equal(rowsPerIteration, rowCount);

            int currentRows = rowsPerIteration;
            for (int i = 0; i < numIterations; i++)
            {
                var tablets = await client.GetTableLocationsAsync(
                    table.TableId, Array.Empty<byte>(), 1);
                Assert.Single(tablets);

                if (restart)
                    await harness.RestartTabletServerAsync(tablets[0]);
                else
                    await harness.KillTabletLeaderAsync(tablets[0]);

                for (int j = 0; j < rowsPerIteration; j++)
                {
                    var row = ClientTestUtil.CreateBasicSchemaInsert(table, currentRows);
                    await session.EnqueueAsync(row);
                    currentRows++;
                }

                await session.FlushAsync();

                if (!restart)
                    harness.StartAllTabletServers();

                rowCount = await ClientTestUtil.CountRowsAsync(client, table);
                Assert.Equal(currentRows, rowCount);
            }

            rowCount = await ClientTestUtil.CountRowsAsync(client, table);
            Assert.Equal(totalRowsToInsert, rowCount);
        }
    }
}
