using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client
{
    /// <summary>
    /// <para>
    /// The external consistency mode for client requests.
    /// This defines how transactions and/or sequences of operations that touch
    /// several TabletServers, in different machines, can be observed by external
    /// clients.
    /// </para>
    /// 
    /// <para>
    /// Note that ExternalConsistencyMode makes no guarantee on atomicity, i.e.
    /// no sequence of operations is made atomic (or transactional) just because
    /// an external consistency mode is set.
    /// Note also that ExternalConsistencyMode has no implication on the
    /// consistency between replicas of the same tablet.
    /// </para>
    /// </summary>
    public enum ExternalConsistencyMode
    {
        /// <summary>
        /// <para>
        /// The response to any write will contain a timestamp. Any further calls
        /// from the same client to other servers will update those servers
        /// with that timestamp. Following write operations from the same client
        /// will be assigned timestamps that are strictly higher, enforcing external
        /// consistency without having to wait or incur any latency penalties.
        /// </para>
        /// 
        /// <para>
        /// In order to maintain external consistency for writes between
        /// two different clients in this mode, the user must forward the timestamp
        /// from the first client to the second by using
        /// <see cref="KuduClient.LastPropagatedTimestamp"/>.
        /// </para>
        /// 
        /// <para>
        /// This is the default external consistency mode.
        /// </para>
        /// 
        /// <para>
        /// Failure to propagate timestamp information through back-channels
        /// between two different clients will negate any external consistency
        /// guarantee under this mode.
        /// </para>
        /// </summary>
        ClientPropagated = ExternalConsistencyModePB.ClientPropagated,

        /// <summary>
        /// <para>
        /// The server will guarantee that write operations from the same or from
        /// other client are externally consistent, without the need to propagate
        /// timestamps across clients. This is done by making write operations
        /// wait until there is certainty that all follow up write operations
        /// (operations that start after the previous one finishes)
        /// will be assigned a timestamp that is strictly higher, enforcing external
        /// consistency.
        /// </para>
        /// 
        /// <para>
        /// Depending on the clock synchronization state of TabletServers this may
        /// imply considerable latency. Moreover operations in COMMIT_WAIT
        /// external consistency mode will outright fail if TabletServer clocks
        /// are either unsynchronized or synchronized but with a maximum error
        /// which surpasses a pre-configured threshold.
        /// </para>
        /// </summary>
        CommitWait = ExternalConsistencyModePB.CommitWait
    }
}
