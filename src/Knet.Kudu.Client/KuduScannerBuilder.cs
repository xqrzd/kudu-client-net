using Knet.Kudu.Client.Scanner;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public class KuduScannerBuilder<T> : AbstractKuduScannerBuilder<KuduScannerBuilder<T>>
    {
        private static readonly ResultSetScanParserFactory _resultSetScanParserFactory =
            new ResultSetScanParserFactory();

        private static readonly ColumnarResultSetScanParserFactory _columnarResultSetScanParserFactory =
            new ColumnarResultSetScanParserFactory();

        internal readonly ILogger Logger;
        internal IKuduScanParserFactory<T> ScanParser;

        public KuduScannerBuilder(KuduClient client, KuduTable table, ILogger logger)
            : base(client, table)
        {
            Logger = logger;
        }

        public KuduScannerBuilder<T> SetScanParser(IKuduScanParserFactory<T> scanParser)
        {
            ScanParser = scanParser;
            return this;
        }

        public KuduScanner<T> Build()
        {
            if (ScanParser is null)
            {
                if (typeof(T) == typeof(ResultSet))
                    ScanParser = (IKuduScanParserFactory<T>)_resultSetScanParserFactory;
                else if (typeof(T) == typeof(ColumnarResultSet))
                    ScanParser = (IKuduScanParserFactory<T>)_columnarResultSetScanParserFactory;
            }

            return new KuduScanner<T>(
                Logger,
                Client,
                Table,
                ScanParser,
                ProjectedColumnNames,
                ProjectedColumnIndexes,
                Predicates,
                ReadMode,
                ReplicaSelection,
                ScanParser.RowFormat,
                IsFaultTolerant,
                BatchSizeBytes,
                Limit,
                CacheBlocks,
                StartTimestamp,
                HtTimestamp,
                LowerBoundPrimaryKey,
                UpperBoundPrimaryKey,
                LowerBoundPartitionKey,
                UpperBoundPartitionKey);
        }
    }
}
