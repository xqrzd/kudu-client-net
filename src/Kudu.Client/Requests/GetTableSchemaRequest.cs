using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class GetTableSchemaRequest
        : KuduMasterRpc<GetTableSchemaRequestPB, GetTableSchemaResponsePB>
    {
        public override string MethodName => "GetTableSchema";

        public GetTableSchemaRequest(GetTableSchemaRequestPB request)
        {
            Request = request;
        }
    }
}
