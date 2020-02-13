using System.Collections.Generic;
using System.Threading;
using Knet.Kudu.Client.Scanner;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public class KuduScanner<T> : IAsyncEnumerable<T>
    {
        private readonly ILogger _logger;
        private readonly KuduClient _client;
        private readonly KuduTable _table;
        private readonly IKuduScanParser<T> _parser; // TODO: This should be a factory.
        private readonly List<string> _projectedColumns;
        private readonly Dictionary<string, KuduPredicate> _predicates;

        private readonly ReadMode _readMode;
        private readonly ReplicaSelection _replicaSelection;
        private readonly bool _isFaultTolerant;
        private readonly int? _batchSizeBytes;
        private readonly long _limit;
        private readonly bool _cacheBlocks;
        private readonly long _startTimestamp;
        private readonly long _htTimestamp;

        private readonly byte[] _lowerBoundPrimaryKey;
        private readonly byte[] _upperBoundPrimaryKey;
        private readonly byte[] _lowerBoundPartitionKey;
        private readonly byte[] _upperBoundPartitionKey;

        public KuduScanner(
            ILogger logger,
            KuduClient client,
            KuduTable table,
            IKuduScanParser<T> parser,
            List<string> projectedColumns,
            Dictionary<string, KuduPredicate> predicates,
            ReadMode readMode,
            ReplicaSelection replicaSelection,
            bool isFaultTolerant,
            int? batchSizeBytes,
            long limit,
            bool cacheBlocks,
            long startTimestamp,
            long htTimestamp,
            byte[] lowerBoundPrimaryKey,
            byte[] upperBoundPrimaryKey,
            byte[] lowerBoundPartitionKey,
            byte[] upperBoundPartitionKey)
        {
            _logger = logger;
            _client = client;
            _table = table;
            _parser = parser;
            _projectedColumns = projectedColumns;
            _predicates = predicates;
            _readMode = readMode;
            _replicaSelection = replicaSelection;
            _isFaultTolerant = isFaultTolerant;
            _batchSizeBytes = batchSizeBytes;
            _limit = limit;
            _cacheBlocks = cacheBlocks;
            _startTimestamp = startTimestamp;
            _htTimestamp = htTimestamp;
            _lowerBoundPrimaryKey = lowerBoundPrimaryKey;
            _upperBoundPrimaryKey = upperBoundPrimaryKey;
            _lowerBoundPartitionKey = lowerBoundPartitionKey;
            _upperBoundPartitionKey = upperBoundPartitionKey;
        }

        public KuduScanEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            var partitionPruner = PartitionPruner.Create(
                _table.Schema,
                _table.PartitionSchema,
                _predicates,
                _lowerBoundPrimaryKey,
                _upperBoundPrimaryKey,
                _lowerBoundPartitionKey,
                _upperBoundPartitionKey);

            return new KuduScanEnumerator<T>(
                _logger,
                _client,
                _table,
                _parser,
                _projectedColumns,
                _readMode,
                _isFaultTolerant,
                _predicates,
                _limit,
                _cacheBlocks,
                _lowerBoundPrimaryKey,
                _upperBoundPrimaryKey,
                _startTimestamp,
                _htTimestamp,
                _batchSizeBytes,
                partitionPruner,
                _replicaSelection,
                cancellationToken);
        }

        IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return GetAsyncEnumerator(cancellationToken);
        }
    }
}
