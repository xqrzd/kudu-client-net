using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class ListTablesRequest
        : KuduMasterRpc<ListTablesRequestPB, ListTablesResponsePB>
    {
        public override string MethodName => "ListTables";

        public ListTablesRequest(ListTablesRequestPB request)
        {
            Request = request;
        }
    }
}
