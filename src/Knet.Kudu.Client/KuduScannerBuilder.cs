using System;
using Knet.Kudu.Client.Scanner;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public class KuduScannerBuilder<T> : AbstractKuduScannerBuilder<KuduScannerBuilder<T>>
    {
        private static readonly IKuduScanParser<ResultSet> _resultSetParser = new ResultSetParser();
        private static readonly IKuduScanParser<ColumnarResultSet> _columnarResultSetParser = new ColumnarResultSetParser();

        internal readonly ILogger Logger;
        internal IKuduScanParser<T> ScanParser;

        public KuduScannerBuilder(KuduClient client, KuduTable table, ILogger logger)
            : base(client, table)
        {
            Logger = logger;
        }

        public KuduScannerBuilder<T> SetScanParser(IKuduScanParser<T> scanParser)
        {
            ScanParser = scanParser;
            return this;
        }

        public KuduScanner<T> Build()
        {
            if (ScanParser is null)
            {
                if (typeof(T) == typeof(ResultSet))
                    ScanParser = (IKuduScanParser<T>)_resultSetParser;
                else if (typeof(T) == typeof(ColumnarResultSet))
                    ScanParser = (IKuduScanParser<T>)_columnarResultSetParser;
                else
                    throw new ArgumentException($"A scan parser was not supplied for type {typeof(T).Name}");
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
