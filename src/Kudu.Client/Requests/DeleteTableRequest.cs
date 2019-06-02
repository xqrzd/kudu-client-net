using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class DeleteTableRequest : KuduRpc<DeleteTableRequestPB, DeleteTableResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "DeleteTable";

        public DeleteTableRequest(DeleteTableRequestPB request)
        {
            Request = request;
        }
    }
}
