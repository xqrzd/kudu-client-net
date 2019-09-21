using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class ConnectToMasterRequest
        : KuduMasterRpc<ConnectToMasterRequestPB, ConnectToMasterResponsePB>
    {
        public override string MethodName => "ConnectToMaster";
    }
}
