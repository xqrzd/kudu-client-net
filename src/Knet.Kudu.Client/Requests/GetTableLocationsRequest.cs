using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class GetTableLocationsRequest : KuduMasterRpc<GetTableLocationsResponsePB>
    {
        private readonly GetTableLocationsRequestPB _request;

        public override string MethodName => "GetTableLocations";

        public GetTableLocationsRequest(GetTableLocationsRequestPB request)
        {
            _request = request;
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = GetTableLocationsResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
