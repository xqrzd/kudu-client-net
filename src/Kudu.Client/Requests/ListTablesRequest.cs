using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class ListTablesRequest : KuduRpc<ListTablesRequestPB, ListTablesResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "ListTables";

        public ListTablesRequest(ListTablesRequestPB request)
        {
            Request = request;
        }
    }
}
