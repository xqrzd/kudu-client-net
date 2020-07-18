namespace Knet.Kudu.Client.Scanner
{
    public class ResultSetScanParserFactory : IKuduScanParserFactory<ResultSet>
    {
        public KuduScanParser<ResultSet> CreateParser()
        {
            return new ResultSetScanParser();
        }
    }
}
