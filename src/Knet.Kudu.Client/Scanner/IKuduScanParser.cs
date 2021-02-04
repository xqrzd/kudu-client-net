using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public interface IKuduScanParser<T>
    {
        RowDataFormat RowFormat { get; }

        KuduScanParserResult<T> ParseSidecars(
            KuduSchema scanSchema,
            ScanResponsePB scanResponse,
            KuduSidecars sidecars);
    }

    public readonly struct KuduScanParserResult<T>
    {
        public T Result { get; }

        public long NumRows { get; }

        public KuduScanParserResult(T result, long numRows)
        {
            Result = result;
            NumRows = numRows;
        }
    }
}
