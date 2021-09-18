using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client;

/// <summary>
/// One of the replicas of the tablet.
/// </summary>
public class KuduReplica
{
    public HostAndPort HostPort { get; }

    public ReplicaRole Role { get; }

    public string DimensionLabel { get; }

    public KuduReplica(HostAndPort hostPort, ReplicaRole role, string dimensionLabel)
    {
        HostPort = hostPort;
        Role = role;
        DimensionLabel = dimensionLabel;
    }

    public override string ToString() =>
        $"Replica(host={HostPort}, role={Role}, dimensionLabel={DimensionLabel})";
}
