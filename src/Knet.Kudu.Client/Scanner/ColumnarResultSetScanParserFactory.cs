namespace Knet.Kudu.Client.Scanner
{
    public class ColumnarResultSetScanParserFactory : IKuduScanParserFactory<ColumnarResultSet>
    {
        public RowDataFormat RowFormat => RowDataFormat.Columnar;

        public KuduScanParser<ColumnarResultSet> CreateParser()
        {
            return new ColumnarResultSetParser();
        }
    }
}
