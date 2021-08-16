using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Requests
{
    internal class WriteRequest : KuduTabletRpc<WriteResponsePB>
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

        public override int CalculateSize()
        {
            if (AuthzToken != null)
                _request.AuthzToken = AuthzToken;

            if (PropagatedTimestamp != KuduClient.NoTimestamp)
                _request.PropagatedTimestamp = (ulong)PropagatedTimestamp;

            _request.TabletId = UnsafeByteOperations.UnsafeWrap(Tablet.TabletId.ToUtf8ByteArray());

            return _request.CalculateSize();
        }

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = WriteResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
