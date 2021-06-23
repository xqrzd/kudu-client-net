using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class DeleteTableRequest : KuduMasterRpc<DeleteTableResponsePB>
    {
        private readonly DeleteTableRequestPB _request;

        public override string MethodName => "DeleteTable";

        public DeleteTableRequest(DeleteTableRequestPB request)
        {
            _request = request;
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = DeleteTableResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
