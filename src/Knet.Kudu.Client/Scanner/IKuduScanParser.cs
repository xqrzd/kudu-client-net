using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public interface IKuduScanParser<T>
    {
        T Output { get; }

        void ProcessScanResponse(KuduSchema scanSchema, ScanResponsePB scanResponse);

        void ProcessSidecar(KuduSidecar sidecar);

        void ParseSidecars(KuduSidecars sidecars); // TODO: Abstract class
    }
}
