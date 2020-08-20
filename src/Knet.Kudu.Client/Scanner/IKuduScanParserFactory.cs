namespace Knet.Kudu.Client.Scanner
{
    public interface IKuduScanParserFactory<T>
    {
        RowDataFormat RowFormat { get; }

        KuduScanParser<T> CreateParser();
    }
}
