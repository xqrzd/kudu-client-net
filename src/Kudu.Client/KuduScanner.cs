using Kudu.Client.Builder;

namespace Kudu.Client
{
    public class KuduScanner
    {
        private readonly ScanBuilder _scanBuilder;

        public KuduScanner(ScanBuilder scanBuilder)
        {
            _scanBuilder = scanBuilder;
        }
    }
}
