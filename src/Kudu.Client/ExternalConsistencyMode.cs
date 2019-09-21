using Kudu.Client.Protocol;

namespace Kudu.Client
{
    /// <summary>
    /// The external consistency mode for client requests.
    /// This defines how transactions and/or sequences of operations that touch
    /// several TabletServers, in different machines, can be observed by external
    /// clients.
    ///
    /// Note that ExternalConsistencyMode makes no guarantee on atomicity, i.e.
    /// no sequence of operations is made atomic (or transactional) just because
    /// an external consistency mode is set.
    /// Note also that ExternalConsistencyMode has no implication on the
    /// consistency between replicas of the same tablet.
    /// </summary>
    public enum ExternalConsistencyMode
    {
        /// <summary>
        /// The response to any write will contain a timestamp.
        /// Any further calls from the same client to other servers will update
        /// those servers with that timestamp. The user will make sure that the
        /// timestamp is propagated through back-channels to other
        /// KuduClient's.
        ///
        /// WARNING: Failure to propagate timestamp information through
        /// back-channels will negate any external consistency guarantee under this
        /// mode.
        ///
        /// Example:
        /// 1 - Client A executes operation X in Tablet A
        /// 2 - Afterwards, Client A executes operation Y in Tablet B
        ///
        ///
        /// Client B may observe the following operation sequences:
        /// {}, {X}, {X Y}
        ///
        /// This is the default mode.
        /// </summary>
        ClientPropagated = ExternalConsistencyModePB.ClientPropagated,

        /// <summary>
        /// The server will guarantee that each transaction is externally
        /// consistent by making sure that none of its results are visible
        /// until every Kudu server agrees that the transaction is in the past.
        /// The client is not obligated to forward timestamp information
        /// through back-channels.
        ///
        /// WARNING: Depending on the clock synchronization state of TabletServers
        /// this may imply considerable latency. Moreover operations with
        /// COMMIT_WAIT requested external consistency will outright fail if
        /// TabletServer clocks are either unsynchronized or synchronized but
        /// with a maximum error which surpasses a pre-configured one.
        ///
        /// Example:
        /// - Client A executes operation X in Tablet A
        /// - Afterwards, Client A executes operation Y in Tablet B
        ///
        ///
        /// Client B may observe the following operation sequences:
        /// {}, {X}, {X Y}
        /// </summary>
        CommitWait = ExternalConsistencyModePB.CommitWait
    }
}
