using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class IsCreateTableDoneRequest : KuduMasterRpc<IsCreateTableDoneResponsePB>
    {
        private readonly IsCreateTableDoneRequestPB _request;

        public override string MethodName => "IsCreateTableDone";

        public IsCreateTableDoneRequest(TableIdentifierPB tableId)
        {
            _request = new IsCreateTableDoneRequestPB { Table = tableId };
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = IsCreateTableDoneResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
