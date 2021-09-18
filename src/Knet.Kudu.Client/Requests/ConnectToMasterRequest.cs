using System.Buffers;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests;

internal sealed class ConnectToMasterRequest : KuduMasterRpc<ConnectToMasterResponsePB>
{
    private static readonly RepeatedField<uint> _requiredFeatures = new()
    {
        (uint)MasterFeatures.ConnectToMaster
    };

    private static readonly byte[] _requestBytes = ProtobufHelper.ToByteArray(
        new ConnectToMasterRequestPB());

    public ConnectToMasterRequest()
    {
        MethodName = "ConnectToMaster";
        RequiredFeatures = _requiredFeatures;
    }

    public override int CalculateSize() => _requestBytes.Length;

    public override void WriteTo(IBufferWriter<byte> output) => output.Write(_requestBytes);

    public override void ParseResponse(KuduMessage message)
    {
        Output = ConnectToMasterResponsePB.Parser.ParseFrom(message.MessageProtobuf);
        Error = Output.Error;
    }
}
