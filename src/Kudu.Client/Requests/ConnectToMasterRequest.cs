using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class ConnectToMasterRequest : KuduRpc<ConnectToMasterRequestPB, ConnectToMasterResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "ConnectToMaster";

        public override ConnectToMasterRequestPB Request => new ConnectToMasterRequestPB();
    }
}
