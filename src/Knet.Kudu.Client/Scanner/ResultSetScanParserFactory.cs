namespace Knet.Kudu.Client.Scanner
{
    public class ResultSetScanParserFactory : IKuduScanParserFactory<ResultSet>
    {
        public RowDataFormat RowFormat => RowDataFormat.Rowwise;

        public KuduScanParser<ResultSet> CreateParser()
        {
            return new ResultSetScanParser();
        }
    }
}
