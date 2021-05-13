using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class KuduPartitionerTests
    {
        [SkippableFact]
        public async Task TestPartitioner()
        {
            await using var miniCluster = await new MiniKuduClusterBuilder().BuildAsync();
            await using var client = miniCluster.CreateClient();

            // Create a table with the following 9 partitions:
            //
            //             hash bucket
            //   key     0      1     2
            //         -----------------
            //  <3333    x      x     x
            // 3333-6666 x      x     x
            //  >=6666   x      x     x

            int numRanges = 3;
            int numHashPartitions = 3;
            var splits = new int[] { 3333, 6666 };

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(nameof(TestPartitioner))
                .AddHashPartitions(numHashPartitions, "key")
                .SetRangePartitionColumns("key");

            foreach (var split in splits)
            {
                builder.AddSplitRow(row => row.SetInt32("key", split));
            }

            var table = await client.CreateTableAsync(builder);

            var partitioner = await client.CreatePartitionerAsync(table);
            int numPartitions = partitioner.NumPartitions;

            Assert.Equal(numRanges * numHashPartitions, numPartitions);

            // Partition a bunch of rows, counting how many fall into each partition.
            int numRowsToPartition = 10000;
            var countsByPartition = new int[numPartitions];
            for (int i = 0; i < numRowsToPartition; i++)
            {
                var row = table.NewInsert();
                row.SetInt32("key", i);
                var partitionIndex = partitioner.PartitionRow(row);
                countsByPartition[partitionIndex]++;
            }

            // We don't expect a completely even division of rows into partitions, but
            // we should be within 10% of that.
            int expectedPerPartition = numRowsToPartition / numPartitions;
            int fuzziness = expectedPerPartition / 10;
            int minPerPartition = expectedPerPartition - fuzziness;
            int maxPerPartition = expectedPerPartition + fuzziness;
            for (int i = 0; i < numPartitions; i++)
            {
                Assert.True(minPerPartition <= countsByPartition[i]);
                Assert.True(maxPerPartition >= countsByPartition[i]);
            }

            // Drop the first and third range partition.
            await client.AlterTableAsync(new AlterTableBuilder(table)
                .DropRangePartition((lower, upper) => upper.SetInt32("key", splits[0]))
                .DropRangePartition((lower, upper) => lower.SetInt32("key", splits[1])));

            // The existing partitioner should still return results based on the table
            // state at the time it was created, and successfully return partitions
            // for rows in the now-dropped range.
            Assert.Equal(numRanges * numHashPartitions, partitioner.NumPartitions);
            var row2 = table.NewInsert();
            row2.SetInt32("key", 1000);
            Assert.Equal(0, partitioner.PartitionRow(row2));

            // If we recreate the partitioner, it should get the new partitioning info.
            partitioner = await client.CreatePartitionerAsync(table);
            Assert.Equal(numHashPartitions, partitioner.NumPartitions);
        }

        [SkippableFact]
        public async Task TestPartitionerNonCoveredRange()
        {
            await using var miniCluster = await new MiniKuduClusterBuilder().BuildAsync();
            await using var client = miniCluster.CreateClient();

            int numHashPartitions = 3;

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(nameof(TestPartitionerNonCoveredRange))
                .AddHashPartitions(numHashPartitions, "key")
                .SetRangePartitionColumns("key");

            // Cover a range where 1000 <= key < 2000
            builder.AddRangePartition((lower, upper) =>
            {
                lower.SetInt32("key", 1000);
                upper.SetInt32("key", 2000);
            });

            var table = await client.CreateTableAsync(builder);

            var partitioner = await client.CreatePartitionerAsync(table);

            Assert.Throws<NonCoveredRangeException>(() =>
            {
                var under = table.NewInsert();
                under.SetInt32("key", 999);
                partitioner.PartitionRow(under);
            });

            Assert.Throws<NonCoveredRangeException>(() =>
            {
                var over = table.NewInsert();
                over.SetInt32("key", 2000);
                partitioner.PartitionRow(over);
            });
        }
    }
}
