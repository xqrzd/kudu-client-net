namespace Knet.Kudu.Client;

/// <summary>
/// Represent statistics belongs to a specific kudu table.
/// </summary>
public class KuduTableStatistics
{
    /// <summary>
    /// The table's on disk size in bytes, this statistic is pre-replication.
    /// </summary>
    public long OnDiskSize { get; }

    /// <summary>
    /// The table's live row count, this statistic is pre-replication.
    /// </summary>
    public long LiveRowCount { get; }

    public KuduTableStatistics(long onDiskSize, long liveRowCount)
    {
        OnDiskSize = onDiskSize;
        LiveRowCount = liveRowCount;
    }
}
