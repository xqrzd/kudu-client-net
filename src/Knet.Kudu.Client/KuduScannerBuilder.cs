using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client;

public class KuduScannerBuilder : AbstractKuduScannerBuilder<KuduScannerBuilder>
{
    internal readonly ILogger Logger;

    public KuduScannerBuilder(KuduClient client, KuduTable table, ILogger logger)
        : base(client, table)
    {
        Logger = logger;
    }

    public KuduScanner Build()
    {
        return new KuduScanner(
            Logger,
            Client,
            Table,
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
