using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;

namespace Knet.Kudu.Client.Requests
{
    public class ConnectToMasterRequest : KuduMasterRpc<ConnectToMasterResponsePB>
    {
        private static readonly uint[] _requiredFeatures = new uint[]
        {
            (uint)MasterFeatures.ConnectToMaster
        };

        private static readonly ConnectToMasterResponsePB _request =
            new ConnectToMasterResponsePB();

        public ConnectToMasterRequest()
        {
            RequiredFeatures = _requiredFeatures;
        }

        public override string MethodName => "ConnectToMaster";

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
