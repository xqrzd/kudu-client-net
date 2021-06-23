using System.Buffers;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class ConnectToMasterRequest : KuduMasterRpc<ConnectToMasterResponsePB>
    {
        private static readonly RepeatedField<uint> _requiredFeatures = new()
        {
            (uint)MasterFeatures.ConnectToMaster
        };

        private static readonly byte[] _requestBytes = new ConnectToMasterRequestPB().ToByteArray();

        public ConnectToMasterRequest()
        {
            RequiredFeatures = _requiredFeatures;
        }

        public override string MethodName => "ConnectToMaster";

        public override int CalculateSize() => _requestBytes.Length;

        public override void WriteTo(IBufferWriter<byte> output) => output.Write(_requestBytes);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = ConnectToMasterResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
