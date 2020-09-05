using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    // TODO: Make this class thread safe.
    public abstract class KuduScanParser<T>
    {
        public long NumRows { get; protected set; }

        public abstract T Output { get; }

        public abstract void ProcessScanResponse(KuduSchema scanSchema, ScanResponsePB scanResponse);

        public virtual void ParseSidecars(KuduSidecars sidecars)
        {
            sidecars.Dispose();
        }
    }
}
