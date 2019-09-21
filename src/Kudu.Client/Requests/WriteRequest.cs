using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client.Requests
{
    public class WriteRequest : KuduTabletRpc<WriteRequestPB, WriteResponsePB>
    {
        public override string MethodName => "Write";

        public WriteRequest(WriteRequestPB request, string tableId)
        {
            Request = request;
            TableId = tableId;
        }
    }
}
