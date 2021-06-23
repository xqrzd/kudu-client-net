using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class ListTabletServersRequest : KuduMasterRpc<ListTabletServersResponsePB>
    {
        private static readonly byte[] _requestBytes = new ListTabletServersRequestPB().ToByteArray();

        public override string MethodName => "ListTabletServers";

        public override int CalculateSize() => _requestBytes.Length;

        public override void WriteTo(IBufferWriter<byte> output) => output.Write(_requestBytes);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = ListTabletServersResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
