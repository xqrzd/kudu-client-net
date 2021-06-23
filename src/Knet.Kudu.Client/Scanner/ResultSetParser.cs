using System;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protobuf.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public sealed class ResultSetParser : IKuduScanParser<ResultSet>
    {
        public RowDataFormat RowFormat => RowDataFormat.Rowwise;

        public KuduScanParserResult<ResultSet> ParseSidecars(
            KuduSchema scanSchema,
            ScanResponsePB scanResponse,
            KuduSidecars sidecars)
        {
            var numRows = scanResponse.Data.NumRows;

            if (sidecars is null)
            {
                var emptyResultSet = new ResultSet(
                    scanSchema, numRows, default, default);

                return new KuduScanParserResult<ResultSet>(
                    emptyResultSet, numRows);
            }

            var rowData = GetRowData(scanResponse, sidecars);
            var indirectData = GetIndirectData(scanResponse, sidecars);

            var resultSet = new ResultSetWrapper(
                sidecars,
                scanSchema,
                numRows,
                rowData,
                indirectData);

            return new KuduScanParserResult<ResultSet>(
                resultSet, numRows);
        }

        private static ReadOnlyMemory<byte> GetRowData(ScanResponsePB responsePb, KuduSidecars sidecars)
        {
            if (responsePb.Data.HasRowsSidecar)
            {
                return sidecars.GetSidecarMemory(responsePb.Data.RowsSidecar);
            }

            return default;
        }

        private static ReadOnlyMemory<byte> GetIndirectData(ScanResponsePB responsePb, KuduSidecars sidecars)
        {
            if (responsePb.Data.HasIndirectDataSidecar)
            {
                return sidecars.GetSidecarMemory(responsePb.Data.IndirectDataSidecar);
            }

            return default;
        }
    }
}
