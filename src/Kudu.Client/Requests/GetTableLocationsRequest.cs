using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class GetTableLocationsRequest
        : KuduMasterRpc<GetTableLocationsRequestPB, GetTableLocationsResponsePB>
    {
        public override string MethodName => "GetTableLocations";

        public GetTableLocationsRequest(GetTableLocationsRequestPB request)
        {
            Request = request;
        }
    }
}
