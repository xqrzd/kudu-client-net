using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests;

internal sealed class GetTableLocationsRequest : KuduMasterRpc<GetTableLocationsResponsePB>
{
    private readonly GetTableLocationsRequestPB _request;

    public GetTableLocationsRequest(GetTableLocationsRequestPB request)
    {
        MethodName = "GetTableLocations";
        _request = request;
    }

    public override int CalculateSize() => _request.CalculateSize();

    public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

    public override void ParseResponse(KuduMessage message)
    {
        Output = GetTableLocationsResponsePB.Parser.ParseFrom(message.MessageProtobuf);
        Error = Output.Error;
    }
}
