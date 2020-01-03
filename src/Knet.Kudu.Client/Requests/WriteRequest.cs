using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Requests
{
    public class WriteRequest : KuduTabletRpc<WriteResponsePB>
    {
        private readonly WriteRequestPB _request;

        public override string MethodName => "Write";

        public WriteRequest(WriteRequestPB request, string tableId, byte[] partitionKey)
        {
            _request = request;
            TableId = tableId;
            PartitionKey = partitionKey;
            NeedsAuthzToken = true;
            IsRequestTracked = true;
        }

        public override void Serialize(Stream stream)
        {
            if (AuthzToken != null)
                _request.AuthzToken = AuthzToken;

            if (PropagatedTimestamp != KuduClient.NoTimestamp)
                _request.PropagatedTimestamp = (ulong)PropagatedTimestamp;

            Serialize(stream, _request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = Deserialize(buffer);
            Error = Output.Error;
        }
    }
}
