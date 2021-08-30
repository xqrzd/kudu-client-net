using Knet.Kudu.Client.Protobuf;

namespace Knet.Kudu.Client
{
    /// <summary>
    /// Policy with which to choose amongst multiple replicas.
    /// </summary>
    public enum ReplicaSelection
    {
        /// <summary>
        /// Select the LEADER replica.
        /// </summary>
        LeaderOnly = ReplicaSelectionPB.LeaderOnly,
        /// <summary>
        /// Select the closest replica to the client. Replicas are classified
        /// from closest to furthest as follows:
        /// <list type="number">
        /// <item><description>Local replicas</description></item>
        /// <item><description>
        /// Replicas whose tablet server has the same location as the client
        /// </description></item>
        /// <item><description>All other replicas</description></item>
        /// </list>
        /// </summary>
        ClosestReplica = ReplicaSelectionPB.ClosestReplica
    }
}
