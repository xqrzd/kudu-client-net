using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public class ColumnarResultSetParser : KuduScanParser<ColumnarResultSet>
    {
        private KuduSchema _scanSchema;
        private ScanResponsePB _responsePB;
        private ColumnarResultSet _result;

        public override ColumnarResultSet Output => _result ?? GetEmptyResultSet();

        public override void ProcessScanResponse(KuduSchema scanSchema, ScanResponsePB scanResponse)
        {
            if (scanResponse.ColumnarData == null)
                throw new NonRecoverableException(
                    KuduStatus.NotSupported("Columnar scan format requires Kudu server 1.12 or newer."));

            _scanSchema = scanSchema;
            _responsePB = scanResponse;
            NumRows = scanResponse.ColumnarData.NumRows;
        }

        public override void ParseSidecars(KuduSidecars sidecars)
        {
            _result = new ColumnarResultSet(
                sidecars,
                _scanSchema,
                _responsePB.ColumnarData);
        }

        private ColumnarResultSet GetEmptyResultSet()
        {
            return new ColumnarResultSet(null, _scanSchema, _responsePB.ColumnarData);
        }
    }
}
