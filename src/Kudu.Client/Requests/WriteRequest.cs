using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client.Requests
{
    public class WriteRequest : KuduRpc<WriteRequestPB, WriteResponsePB>
    {
        public override string ServiceName => TabletServerServiceName;

        public override string MethodName => "Write";

        public WriteRequest(WriteRequestPB request)
        {
            Request = request;
        }
    }
}
