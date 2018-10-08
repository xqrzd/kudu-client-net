using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client.Requests
{
    public class ScanRequest : KuduRpc<ScanRequestPB, ScanResponsePB>
    {
        public override string ServiceName => TabletServerServiceName;

        public override string MethodName => "Scan";

        public override ScanRequestPB Request { get; }

        public ScanRequest(ScanRequestPB request)
        {
            Request = request;
        }
    }
}
