using System.Collections.Generic;
using System.Threading;
using Knet.Kudu.Client.Scanner;

namespace Knet.Kudu.Client
{
    public class KuduScanner : IAsyncEnumerable<ResultSet>
    {
        private readonly ScanBuilder _scanBuilder;

        public KuduScanner(ScanBuilder scanBuilder)
        {
            _scanBuilder = scanBuilder;
        }

        public KuduScanEnumerator GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            // TODO: Get this from the ScanBuilder.
            var parser = new ResultSetScanParser();

            var partitionPruner = PartitionPruner.Create(
                _scanBuilder.Table.Schema,
                _scanBuilder.Table.PartitionSchema,
                _scanBuilder.Predicates,
                _scanBuilder.LowerBoundPrimaryKey,
                _scanBuilder.UpperBoundPrimaryKey,
                _scanBuilder.LowerBoundPartitionKey,
                _scanBuilder.UpperBoundPartitionKey);

            return new KuduScanEnumerator(
                _scanBuilder.Logger,
                _scanBuilder.Client,
                _scanBuilder.Table,
                parser,
                _scanBuilder.ProjectedColumns,
                _scanBuilder.ReadMode,
                _scanBuilder.IsFaultTolerant,
                _scanBuilder.Predicates,
                _scanBuilder.Limit,
                _scanBuilder.CacheBlocks,
                _scanBuilder.LowerBoundPrimaryKey,
                _scanBuilder.UpperBoundPrimaryKey,
                _scanBuilder.StartTimestamp,
                _scanBuilder.HtTimestamp,
                _scanBuilder.BatchSizeBytes,
                partitionPruner,
                _scanBuilder.ReplicaSelection,
                cancellationToken);
        }

        IAsyncEnumerator<ResultSet> IAsyncEnumerable<ResultSet>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return GetAsyncEnumerator(cancellationToken);
        }
    }
}
