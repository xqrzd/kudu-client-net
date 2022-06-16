namespace Knet.Kudu.Client;

/// <summary>
/// The possible read modes for scanners.
/// </summary>
public enum ReadMode
{
    /// <summary>
    /// <para>
    /// When READ_LATEST is specified the server will always return committed writes at
    /// the time the request was received. This type of read does not return a snapshot
    /// timestamp and is not repeatable.
    /// </para>
    /// 
    /// <para>
    /// In ACID terms this corresponds to Isolation mode: "Read Committed".
    /// </para>
    /// 
    /// <para>
    /// This is the default mode.
    /// </para>
    /// </summary>
    ReadLatest = Protobuf.ReadMode.ReadLatest,
    /// <summary>
    /// <para>
    /// When READ_AT_SNAPSHOT is specified the server will attempt to perform a read
    /// at the provided timestamp. If no timestamp is provided the server will take the
    /// current time as the snapshot timestamp. In this mode reads are repeatable, i.e.
    /// all future reads at the same timestamp will yield the same data. This is performed
    /// at the expense of waiting for in-flight transactions whose timestamp is lower
    /// than the snapshot's timestamp to complete, so it might incur a latency penalty.
    /// </para>
    ///
    /// <para>
    /// In ACID terms this, by itself, corresponds to Isolation mode "Repeatable
    /// Read". If all writes to the scanned tablet are made externally consistent,
    /// then this corresponds to Isolation mode "Strict-Serializable".
    /// </para>
    /// </summary>
    ReadAtSnapshot = Protobuf.ReadMode.ReadAtSnapshot,
    /// <summary>
    /// <para>
    /// When READ_YOUR_WRITES is specified, the client will perform a read
    /// such that it follows all previously known writes and reads from this client.
    /// Specifically this mode:
    /// </para>
    /// 
    /// <list type="number">
    /// <item><description>
    /// Ensures read-your-writes and read-your-reads session guarantees,
    /// </description></item>
    /// <item><description>
    /// Minimizes latency caused by waiting for outstanding write transactions to complete.
    /// </description></item>
    /// </list>
    /// 
    /// <para>
    /// Reads in this mode are not repeatable: two READ_YOUR_WRITES reads, even if
    /// they provide the same propagated timestamp bound, can execute at different
    /// timestamps and thus may return different results.
    /// </para>
    /// </summary>
    ReadYourWrites = Protobuf.ReadMode.ReadYourWrites
}
