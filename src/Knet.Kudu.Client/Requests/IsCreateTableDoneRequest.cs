using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal class IsCreateTableDoneRequest : KuduMasterRpc<IsCreateTableDoneResponsePB>
    {
        private readonly IsCreateTableDoneRequestPB _request;

        public override string MethodName => "IsCreateTableDone";

        public IsCreateTableDoneRequest(TableIdentifierPB tableId)
        {
            _request = new IsCreateTableDoneRequestPB { Table = tableId };
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = IsCreateTableDoneResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
