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
        /// - Local replicas
        /// - Replicas whose tablet server has the same location as the client
        /// - All other replicas
        /// </summary>
        ClosestReplica = ReplicaSelectionPB.ClosestReplica
    }
}
