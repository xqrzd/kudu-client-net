using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    class GetTableLocationsRequest : KuduMasterRpc<GetTableLocationsRequestPB, GetTableLocationsResponsePB>
    {
        public override string MethodName => "GetTableLocations";

        public override GetTableLocationsRequestPB Request { get; }

        public GetTableLocationsRequest(GetTableLocationsRequestPB request)
        {
            Request = request;
        }
    }
}
