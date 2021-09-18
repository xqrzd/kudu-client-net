using Knet.Kudu.Client.Protobuf.Security;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Requests;

internal abstract class KuduTabletRpc<T> : KuduRpc<T>
{
    public long PropagatedTimestamp { get; set; } = KuduClient.NoTimestamp;

    /// <summary>
    /// Returns the partition key this RPC is for.
    /// </summary>
    public byte[] PartitionKey { get; init; }

    public RemoteTablet Tablet { get; internal set; }

    public ReplicaSelection ReplicaSelection { get; init; } = ReplicaSelection.LeaderOnly;

    public bool NeedsAuthzToken { get; init; }

    internal SignedTokenPB AuthzToken { get; set; }

    /// <summary>
    /// The table this RPC is for.
    /// </summary>
    public string TableId { get; init; }

    public TabletServerErrorPB Error { get; protected set; }

    public KuduTabletRpc()
    {
        ServiceName = TabletServerServiceName;
    }
}
