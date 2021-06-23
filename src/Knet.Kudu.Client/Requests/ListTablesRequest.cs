using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    public class ListTablesRequest : KuduMasterRpc<ListTablesResponsePB>
    {
        private readonly ListTablesRequestPB _request;

        public override string MethodName => "ListTables";

        public ListTablesRequest(string nameFilter = null)
        {
            _request = new ListTablesRequestPB
            {
                NameFilter = nameFilter
            };
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = ListTablesResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
