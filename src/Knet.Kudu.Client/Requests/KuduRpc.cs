using System;
using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Protocol.Security;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Tablet;
using ProtoBuf;

namespace Knet.Kudu.Client.Requests
{
    // TODO: These types need a lot of refactoring.
    public abstract class KuduRpc : IDisposable
    {
        // Service names.
        protected const string MasterServiceName = "kudu.master.MasterService";
        protected const string TabletServerServiceName = "kudu.tserver.TabletServerService";

        public abstract string ServiceName { get; }

        public abstract string MethodName { get; }

        public long PropagatedTimestamp { get; set; } = -1;

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
        /// If this RPC needs to be tracked on the client and server-side.
        /// Some RPCs require exactly-once semantics which is enabled by tracking them.
        /// </summary>
        public bool IsRequestTracked { get; protected set; }

        internal long SequenceId { get; set; }

        public virtual ReplicaSelection ReplicaSelection => ReplicaSelection.LeaderOnly;

        //void WriteRequest(IBufferWriter<byte> writer);
        public abstract void Serialize(Stream stream);

        public abstract void ParseProtobuf(ReadOnlySequence<byte> buffer);

        // TODO: Use separate class/struct for this.
        // This method would allocate several sidecars.
        // If at any point parsing fails, we need to cleanup these.
        public virtual void BeginProcessingSidecars(KuduSidecarOffsets sidecars)
        {
        }

        public virtual void ParseSidecarSegment(ref SequenceReader<byte> reader)
        {
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }

    public abstract class KuduRpc<T> : KuduRpc
    {
        public virtual T Output { get; protected set; }

        public static void Serialize<TInput>(Stream stream, TInput value)
        {
            Serializer.SerializeWithLengthPrefix(stream, value, PrefixStyle.Base128);
        }

        public static T Deserialize(ReadOnlySequence<byte> buffer)
        {
            return Serializer.Deserialize<T>(buffer);
        }
    }

    public abstract class KuduMasterRpc<T> : KuduRpc<T>
    {
        public override string ServiceName => MasterServiceName;

        public MasterErrorPB Error { get; protected set; }
    }

    public abstract class KuduTabletRpc<T> : KuduRpc<T>
    {
        public override string ServiceName => TabletServerServiceName;

        /// <summary>
        /// Returns the partition key this RPC is for, or null if
        /// the RPC is not tablet specific.
        /// </summary>
        public byte[] PartitionKey { get; protected set; }

        public RemoteTablet Tablet { get; internal set; }

        public bool NeedsAuthzToken { get; protected set; }

        internal SignedTokenPB AuthzToken { get; set; }

        /// <summary>
        /// Used to fetch an authz token if needed.
        /// </summary>
        public string TableId { get; protected set; }

        public TabletServerErrorPB Error { get; protected set; }
    }
}
