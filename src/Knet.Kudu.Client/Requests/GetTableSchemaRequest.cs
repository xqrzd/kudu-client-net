using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;

namespace Knet.Kudu.Client.Requests
{
    public class GetTableSchemaRequest : KuduMasterRpc<GetTableSchemaResponsePB>
    {
        private static uint[] _authzTokenRequiredFeature = new uint[]
        {
            (uint)MasterFeatures.GenerateAuthzToken
        };

        private readonly GetTableSchemaRequestPB _request;

        public override string MethodName => "GetTableSchema";

        public GetTableSchemaRequest(
            GetTableSchemaRequestPB request,
            bool requiresAuthzTokenSupport)
        {
            _request = request;

            if (requiresAuthzTokenSupport)
                RequiredFeatures = _authzTokenRequiredFeature;
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
