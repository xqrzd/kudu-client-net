using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class GetTableLocationsRequest : KuduRpc<GetTableLocationsRequestPB, GetTableLocationsResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "GetTableLocations";

        public GetTableLocationsRequest(GetTableLocationsRequestPB request)
        {
            Request = request;
        }
    }
}
