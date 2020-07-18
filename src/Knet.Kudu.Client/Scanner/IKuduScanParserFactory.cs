namespace Knet.Kudu.Client.Scanner
{
    public interface IKuduScanParserFactory<T>
    {
        KuduScanParser<T> CreateParser();
    }
}
