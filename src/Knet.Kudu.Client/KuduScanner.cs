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
        private readonly IKuduScanParser<T> _parser;
        private readonly List<string> _projectedColumnNames;
        private readonly List<int> _projectedColumnIndexes;
        private readonly Dictionary<string, KuduPredicate> _predicates;

        private readonly RowDataFormat _rowDataFormat;
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

        public ReplicaSelection ReplicaSelection { get; }

        public ReadMode ReadMode { get; }

        public KuduScanner(
            ILogger logger,
            KuduClient client,
            KuduTable table,
            IKuduScanParser<T> parser,
            List<string> projectedColumnNames,
            List<int> projectedColumnIndexes,
            Dictionary<string, KuduPredicate> predicates,
            ReadMode readMode,
            ReplicaSelection replicaSelection,
            RowDataFormat rowDataFormat,
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
            _projectedColumnNames = projectedColumnNames;
            _projectedColumnIndexes = projectedColumnIndexes;
            _predicates = predicates;
            ReadMode = readMode;
            ReplicaSelection = replicaSelection;
            _rowDataFormat = rowDataFormat;
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
                _projectedColumnNames,
                _projectedColumnIndexes,
                ReadMode,
                _rowDataFormat,
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
                ReplicaSelection,
                cancellationToken);
        }

        IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            return GetAsyncEnumerator(cancellationToken);
        }
    }
}
