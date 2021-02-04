using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;

namespace Knet.Kudu.Client.Requests
{
    public class ListTabletServersRequest : KuduMasterRpc<ListTabletServersResponsePB>
    {
        private static readonly ListTabletServersRequestPB _request =
            new ListTabletServersRequestPB();

        public override string MethodName => "ListTabletServers";

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
