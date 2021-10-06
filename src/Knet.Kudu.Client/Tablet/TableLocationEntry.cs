using System.Diagnostics.CodeAnalysis;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Tablet;

public class TableLocationEntry
{
    /// <summary>
    /// The lower bound partition key.
    /// </summary>
    public byte[] LowerBoundPartitionKey { get; }

    /// <summary>
    /// The upper bound partition key.
    /// </summary>
    public byte[] UpperBoundPartitionKey { get; }

    /// <summary>
    /// The remote tablet, only set if this entry represents a tablet.
    /// </summary>
    public RemoteTablet? Tablet { get; }

    /// <summary>
    /// When this entry will expire, based on <see cref="ISystemClock"/>.
    /// </summary>
    public long Expiration { get; }

    public TableLocationEntry(
        RemoteTablet? tablet,
        byte[] lowerBoundPartitionKey,
        byte[] upperBoundPartitionKey,
        long expiration)
    {
        Tablet = tablet;
        LowerBoundPartitionKey = lowerBoundPartitionKey;
        UpperBoundPartitionKey = upperBoundPartitionKey;
        Expiration = expiration;
    }

    /// <summary>
    /// If this entry is a non-covered range.
    /// </summary>
    [MemberNotNullWhen(false, nameof(Tablet))]
    public bool IsNonCoveredRange => Tablet is null;

    /// <summary>
    /// If this entry is a covered range.
    /// </summary>
    [MemberNotNullWhen(true, nameof(Tablet))]
    public bool IsCoveredRange => Tablet is not null;

    public static TableLocationEntry NewNonCoveredRange(
        byte[] lowerBoundPartitionKey,
        byte[] upperBoundPartitionKey,
        long expiration)
    {
        return new TableLocationEntry(
            null,
            lowerBoundPartitionKey,
            upperBoundPartitionKey,
            expiration);
    }

    public static TableLocationEntry NewCoveredRange(RemoteTablet tablet, long expiration)
    {
        var partition = tablet.Partition;
        var lowerBoundPartitionKey = partition.PartitionKeyStart;
        var upperBoundPartitionKey = partition.PartitionKeyEnd;

        return new TableLocationEntry(
            tablet, lowerBoundPartitionKey, upperBoundPartitionKey, expiration);
    }
}
