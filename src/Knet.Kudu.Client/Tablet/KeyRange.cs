using System;

namespace Knet.Kudu.Client.Tablet;

/// <summary>
/// Class used to represent primary key range in tablet.
/// </summary>
public class KeyRange
{
    /// <summary>
    /// The tablet which the key range belongs to.
    /// </summary>
    public RemoteTablet Tablet { get; }

    /// <summary>
    /// The encoded primary key where the key range starts (inclusive).
    /// </summary>
    public ReadOnlyMemory<byte> PrimaryKeyStart { get; }

    /// <summary>
    /// The encoded primary key where the key range stops (exclusive).
    /// </summary>
    public ReadOnlyMemory<byte> PrimaryKeyEnd { get; }

    /// <summary>
    /// The estimated data size of the key range.
    /// </summary>
    public long DataSizeBytes { get; }

    /// <summary>
    /// Create a new key range [primaryKeyStart, primaryKeyEnd).
    /// </summary>
    /// <param name="tablet">The tablet which the key range belongs to.</param>
    /// <param name="primaryKeyStart">
    /// The encoded primary key where to start in the key range (inclusive).
    /// </param>
    /// <param name="primaryKeyEnd">
    /// The encoded primary key where to stop in the key range (exclusive).
    /// </param>
    /// <param name="dataSizeBytes">The estimated data size of the key range.</param>
    public KeyRange(
        RemoteTablet tablet,
        ReadOnlyMemory<byte> primaryKeyStart,
        ReadOnlyMemory<byte> primaryKeyEnd,
        long dataSizeBytes)
    {
        Tablet = tablet;
        PrimaryKeyStart = primaryKeyStart;
        PrimaryKeyEnd = primaryKeyEnd;
        DataSizeBytes = dataSizeBytes;
    }

    /// <summary>
    /// The start partition key.
    /// </summary>
    public byte[] PartitionKeyStart => Tablet.Partition.PartitionKeyStart;

    /// <summary>
    /// The end partition key.
    /// </summary>
    public byte[] PartitionKeyEnd => Tablet.Partition.PartitionKeyEnd;

    public override string ToString()
    {
        var start = PrimaryKeyStart.Length == 0
            ? "<start>"
            : BitConverter.ToString(PrimaryKeyStart.ToArray());

        var end = PrimaryKeyEnd.Length == 0
            ? "<end>"
            : BitConverter.ToString(PrimaryKeyEnd.ToArray());

        return $"[{start}, {end}), {DataSizeBytes}, {Tablet.TabletId} {Tablet.Partition}";
    }
}
