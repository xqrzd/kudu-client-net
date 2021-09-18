using System;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client.Tablet;

/// <summary>
/// <para>
/// A partition describes the set of rows that a tablet is responsible
/// for serving. Each tablet is assigned a single partition.
/// </para>
///
/// <para>
/// Partitions consist primarily of a start and end key.
/// Every row with a partition key that falls in a tablet's partition
/// will be served by that tablet.
/// </para>
///
/// <para>
/// In addition to the start and end partition keys, a partition
/// holds metadata to determine if a scan can prune, or skip, a partition
/// based on the scan's start and end primary keys, and predicates.
/// </para>
/// </summary>
public class Partition : IEquatable<Partition>, IComparable<Partition>
{
    /// <summary>
    /// Size of an encoded hash bucket component in a partition key.
    /// </summary>
    private const int EncodedBucketSize = 4;

    public byte[] PartitionKeyStart { get; }

    public byte[] PartitionKeyEnd { get; }

    public byte[] RangeKeyStart { get; }

    public byte[] RangeKeyEnd { get; }

    public int[] HashBuckets { get; }

    /// <summary>
    /// Creates a new partition with the provided start and end keys, and hash buckets.
    /// </summary>
    /// <param name="partitionKeyStart">The start partition key.</param>
    /// <param name="partitionKeyEnd">The end partition key.</param>
    /// <param name="hashBuckets">The partition hash buckets.</param>
    public Partition(byte[] partitionKeyStart, byte[] partitionKeyEnd, int[] hashBuckets)
    {
        PartitionKeyStart = partitionKeyStart ?? Array.Empty<byte>();
        PartitionKeyEnd = partitionKeyEnd ?? Array.Empty<byte>();
        HashBuckets = hashBuckets ?? Array.Empty<int>();

        RangeKeyStart = RangeKey(PartitionKeyStart, HashBuckets.Length);
        RangeKeyEnd = RangeKey(PartitionKeyEnd, HashBuckets.Length);
    }

    /// <summary>
    /// True if the partition is the start partition.
    /// </summary>
    public bool IsStartPartition => PartitionKeyStart.Length == 0;

    /// <summary>
    /// True if the partition is the absolute end partition.
    /// </summary>
    public bool IsEndPartition => PartitionKeyEnd.Length == 0;

    /// <summary>
    /// Returns the range key portion of a partition key given the
    /// number of buckets in the partition schema.
    /// </summary>
    /// <param name="partitionKey">The partition key containing the range key.</param>
    /// <param name="numHashBuckets">The number of hash bucket components of the table.</param>
    private static byte[] RangeKey(byte[] partitionKey, int numHashBuckets)
    {
        int bucketsLen = numHashBuckets * EncodedBucketSize;

        if (partitionKey.Length > bucketsLen)
        {
            var arr = new byte[partitionKey.Length - bucketsLen];
            partitionKey.AsSpan(bucketsLen).CopyTo(arr);

            return arr;
        }
        else
        {
            return Array.Empty<byte>();
        }
    }

    /// <summary>
    /// Returns true if the given partition key belongs to this partition.
    /// </summary>
    /// <param name="partitionKey">The partition key to check.</param>
    public bool ContainsPartitionKey(ReadOnlySpan<byte> partitionKey)
    {
        return
            (IsStartPartition || partitionKey.SequenceCompareTo(PartitionKeyStart) >= 0) &&
            (IsEndPartition || partitionKey.SequenceCompareTo(PartitionKeyEnd) < 0);
    }

    /// <summary>
    /// Equality only holds for partitions from the same table. Partition equality only takes
    /// into account the partition keys, since there is a 1 to 1 correspondence between
    /// partition keys and the hash buckets and range keys.
    /// </summary>
    /// <param name="other">The other partition of the same table.</param>
    public bool Equals(Partition other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return
            PartitionKeyStart.SequenceEqual(other.PartitionKeyStart) &&
            PartitionKeyEnd.SequenceEqual(other.PartitionKeyEnd);
    }

    /// <summary>
    /// Partition comparison is only reasonable when comparing partitions
    /// from the same table, and since Kudu does not yet allow partition
    /// splitting, no two distinct partitions can have the same start
    /// partition key. Accordingly, partitions are compared strictly by
    /// the start partition key.
    /// </summary>
    /// <param name="other">The other partition of the same table.</param>
    public int CompareTo(Partition other) =>
        PartitionKeyStart.SequenceCompareTo(other.PartitionKeyStart);

    /// <summary>
    /// Equality only holds for partitions from the same table. Partition equality only takes
    /// into account the partition keys, since there is a 1 to 1 correspondence between
    /// partition keys and the hash buckets and range keys.
    /// </summary>
    /// <param name="obj">The other partition of the same table.</param>
    public override bool Equals(object obj) => Equals(obj as Partition);

    /// <summary>
    /// The hash code only takes into account the partition keys, since there is a 1 to 1
    /// correspondence between partition keys and the hash buckets and range keys.
    /// </summary>
    public override int GetHashCode() => PartitionKeyStart.GetContentHashCode();

    public override string ToString()
    {
        var start = IsStartPartition ? "<start>" : BitConverter.ToString(PartitionKeyStart);
        var end = IsEndPartition ? "<end>" : BitConverter.ToString(PartitionKeyEnd);

        return $"[{start}, {end})";
    }
}
