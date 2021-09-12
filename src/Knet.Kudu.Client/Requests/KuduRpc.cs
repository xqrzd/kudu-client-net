using System;
using System.Buffers;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protobuf.Security;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Requests
{
    // TODO: These types need a lot of refactoring.
    public abstract class KuduRpc
    {
        // Service names.
        protected const string MasterServiceName = "kudu.master.MasterService";
        protected const string TabletServerServiceName = "kudu.tserver.TabletServerService";
        protected const string TxnManagerServiceName = "kudu.transactions.TxnManagerService";

        public abstract string ServiceName { get; }

        public abstract string MethodName { get; }

        /// <summary>
        /// Returns the set of application-specific feature flags required to service the RPC.
        /// </summary>
        public RepeatedField<uint> RequiredFeatures { get; protected set; }

        /// <summary>
        /// The external consistency mode for this RPC.
        /// TODO make this cover most if not all RPCs (right now only scans and writes use this).
        /// </summary>
        public ExternalConsistencyMode ExternalConsistencyMode { get; set; } =
            ExternalConsistencyMode.ClientPropagated;

        /// <summary>
        /// The number of times this RPC has been retried.
        /// </summary>
        internal int Attempt { get; set; }

        /// <summary>
        /// The last exception when handling this RPC.
        /// </summary>
        internal Exception Exception { get; set; }

        /// <summary>
        /// If this RPC needs to be tracked on the client and server-side.
        /// Some RPCs require exactly-once semantics which is enabled by tracking them.
        /// </summary>
        public bool IsRequestTracked { get; protected set; }

        internal long SequenceId { get; set; } = RequestTracker.NoSeqNo;

        public abstract int CalculateSize();

        public abstract void WriteTo(IBufferWriter<byte> output);

        public abstract void ParseResponse(KuduMessage message);
    }

    public abstract class KuduRpc<T> : KuduRpc
    {
        public T Output { get; protected set; }
    }

    internal abstract class KuduMasterRpc<T> : KuduRpc<T>
    {
        public override string ServiceName => MasterServiceName;

        public MasterErrorPB Error { get; protected set; }
    }

    internal abstract class KuduTxnRpc<T> : KuduRpc<T>
    {
        public override string ServiceName => TxnManagerServiceName;

        public TxnManagerErrorPB Error { get; protected set; }
    }

    internal abstract class KuduTabletRpc<T> : KuduRpc<T>
    {
        public override string ServiceName => TabletServerServiceName;

        public long PropagatedTimestamp { get; set; } = KuduClient.NoTimestamp;

        /// <summary>
        /// Returns the partition key this RPC is for.
        /// </summary>
        public byte[] PartitionKey { get; protected set; }

        public RemoteTablet Tablet { get; internal set; }

        public virtual ReplicaSelection ReplicaSelection => ReplicaSelection.LeaderOnly;

        public bool NeedsAuthzToken { get; protected set; }

        internal SignedTokenPB AuthzToken { get; set; }

        /// <summary>
        /// The table this RPC is for.
        /// </summary>
        public string TableId { get; protected set; }

        public TabletServerErrorPB Error { get; protected set; }
    }
}
