using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;

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

        public override void Serialize(Stream stream)
        {
            Serialize(stream, _request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = Deserialize(buffer);
            Error = Output.Error;
        }
    }
}
