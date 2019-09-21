using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class DeleteTableRequest
        : KuduMasterRpc<DeleteTableRequestPB, DeleteTableResponsePB>
    {
        public override string MethodName => "DeleteTable";

        public DeleteTableRequest(DeleteTableRequestPB request)
        {
            Request = request;
        }
    }
}
