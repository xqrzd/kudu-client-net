using System.Buffers;
using System.IO;
using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class ConnectToMasterRequest : KuduMasterRpc<ConnectToMasterResponsePB>
    {
        private static readonly ConnectToMasterResponsePB _request =
            new ConnectToMasterResponsePB();

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
