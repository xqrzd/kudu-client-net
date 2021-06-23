using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class IsAlterTableDoneRequest : KuduMasterRpc<IsAlterTableDoneResponsePB>
    {
        private readonly IsAlterTableDoneRequestPB _request;

        public override string MethodName => "IsAlterTableDone";

        public IsAlterTableDoneRequest(TableIdentifierPB tableId)
        {
            _request = new IsAlterTableDoneRequestPB { Table = tableId };
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = IsAlterTableDoneResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
