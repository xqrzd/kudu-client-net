using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    class ListTablesRequest : KuduMasterRpc<ListTablesRequestPB, ListTablesResponsePB>
    {
        public override string MethodName => "ListTables";

        public override ListTablesRequestPB Request { get; }

        public ListTablesRequest(ListTablesRequestPB request)
        {
            Request = request;
        }
    }
}
