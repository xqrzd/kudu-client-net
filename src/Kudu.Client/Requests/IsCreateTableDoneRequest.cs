using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class IsCreateTableDoneRequest : KuduRpc<IsCreateTableDoneRequestPB, IsCreateTableDoneResponsePB>
    {
        public override string ServiceName => MasterServiceName;

        public override string MethodName => "IsCreateTableDone";

        public override IsCreateTableDoneRequestPB Request { get; }

        public IsCreateTableDoneRequest(IsCreateTableDoneRequestPB request)
        {
            Request = request;
        }
    }
}
