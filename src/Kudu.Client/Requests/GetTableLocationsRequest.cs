using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    class GetTableLocationsRequest : KuduRpc<GetTableLocationsRequestPB, GetTableLocationsResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "GetTableLocations";

        public override GetTableLocationsRequestPB Request { get; }

        public GetTableLocationsRequest(GetTableLocationsRequestPB request)
        {
            Request = request;
        }
    }
}
