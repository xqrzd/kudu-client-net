using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal class DeleteTableRequest : KuduMasterRpc<DeleteTableResponsePB>
    {
        private readonly DeleteTableRequestPB _request;

        public override string MethodName => "DeleteTable";

        public DeleteTableRequest(DeleteTableRequestPB request)
        {
            _request = request;
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = DeleteTableResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
