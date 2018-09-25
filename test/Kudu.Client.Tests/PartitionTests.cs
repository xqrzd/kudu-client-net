using Kudu.Client.Tablet;
using Xunit;

namespace Kudu.Client.Tests
{
    public class PartitionTests
    {
        [Fact]
        public void EmptyPartition()
        {
            var partition = new Partition(
                new byte[0],
                new byte[0],
                new int[0]);

            Assert.True(partition.IsEndPartition);
            Assert.Empty(partition.PartitionKeyStart);
            Assert.Empty(partition.PartitionKeyEnd);

            Assert.Empty(partition.RangeKeyStart);
            Assert.Empty(partition.RangeKeyEnd);

            Assert.Empty(partition.HashBuckets);
        }

        [Fact]
        public void EmptyPartitionsAreEqual()
        {
            var partition1 = new Partition(
                new byte[0],
                new byte[0],
                new int[0]);

            var partition2 = new Partition(
                new byte[0],
                new byte[0],
                new int[0]);

            Assert.Equal(partition1, partition2);
            Assert.Equal(0, partition1.CompareTo(partition2));
            Assert.Equal(partition1.GetHashCode(), partition2.GetHashCode());
        }

        [Fact]
        public void SimpleHashPartitionStart()
        {
            var partition = new Partition(
                new byte[0],
                new byte[] { 0, 0, 0, 1 },
                new int[] { 0 });

            Assert.False(partition.IsEndPartition);
            Assert.Empty(partition.PartitionKeyStart);
            Assert.Equal(new byte[] { 0, 0, 0, 1 }, partition.PartitionKeyEnd);

            Assert.Empty(partition.RangeKeyStart);
            Assert.Empty(partition.RangeKeyEnd);

            Assert.Equal(new int[] { 0 }, partition.HashBuckets);
        }

        [Fact]
        public void SimpleHashPartitionMiddle()
        {
            var partition = new Partition(
                new byte[] { 0, 0, 0, 1 },
                new byte[] { 0, 0, 0, 2 },
                new int[] { 1 });

            Assert.False(partition.IsEndPartition);
            Assert.Equal(new byte[] { 0, 0, 0, 1 }, partition.PartitionKeyStart);
            Assert.Equal(new byte[] { 0, 0, 0, 2 }, partition.PartitionKeyEnd);

            Assert.Empty(partition.RangeKeyStart);
            Assert.Empty(partition.RangeKeyEnd);

            Assert.Equal(new int[] { 1 }, partition.HashBuckets);
        }

        [Fact]
        public void SimpleHashPartitionEnd()
        {
            var partition = new Partition(
                new byte[] { 0, 0, 0, 3 },
                new byte[0],
                new int[] { 3 });

            Assert.True(partition.IsEndPartition);
            Assert.Equal(new byte[] { 0, 0, 0, 3 }, partition.PartitionKeyStart);
            Assert.Empty(partition.PartitionKeyEnd);

            Assert.Empty(partition.RangeKeyStart);
            Assert.Empty(partition.RangeKeyEnd);

            Assert.Equal(new int[] { 3 }, partition.HashBuckets);
        }
    }
}
