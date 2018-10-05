using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class GetTableSchemaRequest : KuduRpc<GetTableSchemaRequestPB, GetTableSchemaResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "GetTableSchema";

        public override GetTableSchemaRequestPB Request { get; }

        public GetTableSchemaRequest(GetTableSchemaRequestPB request)
        {
            Request = request;
        }
    }
}
