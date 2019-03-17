using System;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client.Requests
{
    public class ScanRequest : KuduRpc<ScanRequestPB, ScanResponsePB>
    {
        public override string ServiceName => TabletServerServiceName;

        public override string MethodName => "Scan";

        public KuduTable Table { get; }

        public ScanRequest(ScanRequestPB request, KuduTable table)
        {
            Request = request;
            Table = table;
        }

        public override Task ParseSidecarsAsync(
            ResponseHeader header, PipeReader reader, int length)
        {
            throw new NotImplementedException();
        }
    }
}
