using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class IsCreateTableDoneRequest : KuduMasterRpc<IsCreateTableDoneRequestPB, IsCreateTableDoneResponsePB>
    {
        public override string MethodName => "IsCreateTableDone";

        public override IsCreateTableDoneRequestPB Request { get; }

        public IsCreateTableDoneRequest(IsCreateTableDoneRequestPB request)
        {
            Request = request;
        }
    }
}
