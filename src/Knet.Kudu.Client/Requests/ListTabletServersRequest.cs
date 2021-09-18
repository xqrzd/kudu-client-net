using System.Buffers;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal sealed class ListTabletServersRequest : KuduMasterRpc<ListTabletServersResponsePB>
    {
        private static readonly byte[] _requestBytes = ProtobufHelper.ToByteArray(
            new ListTabletServersRequestPB());

        public ListTabletServersRequest()
        {
            MethodName = "ListTabletServers";
        }

        public override int CalculateSize() => _requestBytes.Length;

        public override void WriteTo(IBufferWriter<byte> output) => output.Write(_requestBytes);

        public override void ParseResponse(KuduMessage message)
        {
            Output = ListTabletServersResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
