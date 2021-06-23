using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Protobuf.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public sealed class ColumnarResultSetParser : IKuduScanParser<ColumnarResultSet>
    {
        public RowDataFormat RowFormat => RowDataFormat.Columnar;

        public KuduScanParserResult<ColumnarResultSet> ParseSidecars(
            KuduSchema scanSchema,
            ScanResponsePB scanResponse,
            KuduSidecars sidecars)
        {
            if (scanResponse.ColumnarData is null)
                throw new NonRecoverableException(
                    KuduStatus.NotSupported("Columnar scan format requires Kudu server 1.12 or newer."));

            var columnarData = scanResponse.ColumnarData;

            var resultSet = new ColumnarResultSet(
                sidecars, scanSchema, columnarData);

            return new KuduScanParserResult<ColumnarResultSet>(
                resultSet, columnarData.NumRows);
        }
    }
}
