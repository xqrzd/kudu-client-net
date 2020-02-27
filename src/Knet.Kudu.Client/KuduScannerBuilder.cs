using Knet.Kudu.Client.Scanner;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public class KuduScannerBuilder : AbstractKuduScannerBuilder<KuduScannerBuilder>
    {
        internal readonly ILogger Logger;

        public KuduScannerBuilder(KuduClient client, KuduTable table, ILogger logger)
            : base(client, table)
        {
            Logger = logger;
        }

        public KuduScanner<ResultSet> Build()
        {
            return new KuduScanner<ResultSet>(
                Logger,
                Client,
                Table,
                new ResultSetScanParser(),
                ProjectedColumnNames,
                ProjectedColumnIndexes,
                Predicates,
                ReadMode,
                ReplicaSelection,
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
