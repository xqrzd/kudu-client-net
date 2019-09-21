using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Protocol.Security;
using Kudu.Client.Tablet;
using ProtoBuf;

namespace Kudu.Client.Requests
{
    // TODO: These types need a lot of refactoring.
    public abstract class KuduRpc
    {
        // Service names.
        protected const string MasterServiceName = "kudu.master.MasterService";
        protected const string TabletServerServiceName = "kudu.tserver.TabletServerService";

        public abstract string ServiceName { get; }

        public abstract string MethodName { get; }

        public bool NeedsAuthzToken { get; protected set; }

        internal SignedTokenPB AuthzToken { get; set; }

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

        public bool IsMasterRpc => ServiceName == MasterServiceName;

        public virtual ReplicaSelection ReplicaSelection => ReplicaSelection.LeaderOnly;

        // TODO: Include numAttempts, externalConsistencyMode, etc.

        // TODO: Eventually change this method to
        // public abstract void WriteRequest(IBufferWriter<byte> writer);
        public abstract void WriteRequest(Stream stream);

        public abstract void ParseProtobuf(ReadOnlySequence<byte> buffer);

        public virtual Task ParseSidecarsAsync(ResponseHeader header, PipeReader reader, int length)
        {
            throw new NotImplementedException();
        }

        public static void Serialize<T>(Stream stream, T value)
        {
            Serializer.SerializeWithLengthPrefix(stream, value, PrefixStyle.Base128);
        }

        public static T Parse<T>(ReadOnlySequence<byte> buffer)
        {
            // TODO: This commented code has a parsing bug for fragmented sequences.
            //using var reader = ProtoReader.Create(
            //    out var state, buffer, RuntimeTypeModel.Default);

            //return reader.Deserialize<T>(ref state);

            var array = buffer.ToArray();
            var stream = new MemoryStream(array);
            return Serializer.Deserialize<T>(stream);
        }
    }

    public abstract class KuduMasterRpc : KuduRpc
    {
        public override string ServiceName => MasterServiceName;
    }

    public abstract class KuduMasterRpc<TRequest, TResponse> : KuduMasterRpc
    {
        public TRequest Request { get; set; }

        public TResponse Response { get; set; }

        public override void WriteRequest(Stream stream)
        {
            Serialize(stream, Request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Response = Parse<TResponse>(buffer);
        }
    }

    public abstract class KuduTabletRpc : KuduRpc
    {
        public override string ServiceName => TabletServerServiceName;

        /// <summary>
        /// Returns the partition key this RPC is for, or null if
        /// the RPC is not tablet specific.
        /// </summary>
        public byte[] PartitionKey { get; protected set; }

        public string TableId { get; protected set; }

        public RemoteTablet Tablet { get; internal set; }
    }

    public abstract class KuduTabletRpc<TRequest, TResponse> : KuduTabletRpc
    {
        public TRequest Request { get; set; }

        public TResponse Response { get; set; }

        public override void WriteRequest(Stream stream)
        {
            Serialize(stream, Request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Response = Parse<TResponse>(buffer);
        }
    }
}
