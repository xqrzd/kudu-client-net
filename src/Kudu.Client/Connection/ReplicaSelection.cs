namespace Kudu.Client.Connection
{
    /// <summary>
    /// Policy with which to choose amongst multiple replicas.
    /// </summary>
    public enum ReplicaSelection
    {
        /// <summary>
        /// Select the LEADER replica.
        /// </summary>
        LeaderOnly,
        /// <summary>
        /// Select the closest replica to the client, or a random one if all replicas are equidistant.
        /// </summary>
        ClosestReplica
    }
}
