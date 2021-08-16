using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal class IsAlterTableDoneRequest : KuduMasterRpc<IsAlterTableDoneResponsePB>
    {
        private readonly IsAlterTableDoneRequestPB _request;

        public override string MethodName => "IsAlterTableDone";

        public IsAlterTableDoneRequest(TableIdentifierPB tableId)
        {
            _request = new IsAlterTableDoneRequestPB { Table = tableId };
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = IsAlterTableDoneResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
