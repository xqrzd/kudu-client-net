using System;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public class ResultSetScanParser : KuduScanParser<ResultSet>
    {
        private KuduSchema _scanSchema;
        private ScanResponsePB _responsePB;
        private ResultSetWrapper _result;

        public override ResultSet Output => _result ?? GetEmptyResultSet();

        public override void ProcessScanResponse(KuduSchema scanSchema, ScanResponsePB scanResponse)
        {
            _scanSchema = scanSchema;
            _responsePB = scanResponse;
            NumRows = scanResponse.Data.NumRows;
        }

        public override void ParseSidecars(KuduSidecars sidecars)
        {
            var numRows = _responsePB.Data.NumRows;
            var rowData = GetRowData(sidecars);
            var indirectData = GetIndirectData(sidecars);

            _result = new ResultSetWrapper(
                sidecars,
                _scanSchema,
                numRows,
                rowData,
                indirectData);
        }

        private ReadOnlyMemory<byte> GetRowData(KuduSidecars sidecars)
        {
            if (_responsePB.Data.ShouldSerializeRowsSidecar())
            {
                return sidecars.GetSidecarMemory(_responsePB.Data.RowsSidecar);
            }

            return default;
        }

        private ReadOnlyMemory<byte> GetIndirectData(KuduSidecars sidecars)
        {
            if (_responsePB.Data.ShouldSerializeIndirectDataSidecar())
            {
                return sidecars.GetSidecarMemory(_responsePB.Data.IndirectDataSidecar);
            }

            return default;
        }

        private ResultSet GetEmptyResultSet()
        {
            return new ResultSet(
                _scanSchema,
                _responsePB.Data.NumRows,
                default,
                default);
        }
    }
}
