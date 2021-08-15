using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal class ListTablesRequest : KuduMasterRpc<ListTablesResponsePB>
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

        public override void ParseResponse(KuduMessage message)
        {
            Output = ListTablesResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
