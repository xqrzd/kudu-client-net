using Knet.Kudu.Client.Connection;
using static Knet.Kudu.Client.Protocol.Consensus.RaftPeerPB;

namespace Knet.Kudu.Client.Tablet
{
    /// <summary>
    /// One of the replicas of the tablet.
    /// </summary>
    public class Replica
    {
        public HostAndPort HostPort { get; }

        public Role Role { get; }

        public string DimensionLabel { get; }

        public Replica(HostAndPort hostPort, Role role, string dimensionLabel)
        {
            HostPort = hostPort;
            Role = role;
            DimensionLabel = dimensionLabel;
        }

        public override string ToString() =>
            $"Replica(host={HostPort}, role={Role}, dimensionLabel={DimensionLabel})";
    }
}
