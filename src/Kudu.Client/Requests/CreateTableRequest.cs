using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class CreateTableRequest
        : KuduMasterRpc<CreateTableRequestPB, CreateTableResponsePB>
    {
        public override string MethodName => "CreateTable";

        public CreateTableRequest(CreateTableRequestPB request)
        {
            Request = request;
        }
    }
}
