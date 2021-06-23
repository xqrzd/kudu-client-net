using System.Buffers;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class GetTableSchemaRequest : KuduMasterRpc<GetTableSchemaResponsePB>
    {
        private static readonly RepeatedField<uint> _requiredFeatures = new()
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
                RequiredFeatures = _requiredFeatures;
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = GetTableSchemaResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
