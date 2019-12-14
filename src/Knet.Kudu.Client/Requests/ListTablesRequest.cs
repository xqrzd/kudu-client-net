using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;

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
