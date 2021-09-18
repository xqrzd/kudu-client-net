using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Scanner
{
    public class PartitionPruner
    {
        private readonly Stack<PartitionKeyRange> _rangePartitions;

        public PartitionPruner(Stack<PartitionKeyRange> rangePartitions)
        {
            _rangePartitions = rangePartitions;
        }

        /// <summary>
        /// A partition pruner that will prune all partitions.
        /// </summary>
        private static PartitionPruner Empty =>
            new PartitionPruner(new Stack<PartitionKeyRange>());

        public static PartitionPruner Create<TBuilder>(
            AbstractKuduScannerBuilder<TBuilder> scanBuilder)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder>
        {
            return Create(
                scanBuilder.Table.Schema,
                scanBuilder.Table.PartitionSchema,
                scanBuilder.Predicates,
                scanBuilder.LowerBoundPrimaryKey,
                scanBuilder.UpperBoundPrimaryKey,
                scanBuilder.LowerBoundPartitionKey,
                scanBuilder.UpperBoundPartitionKey);
        }

        public static PartitionPruner Create(
            KuduSchema schema,
            PartitionSchema partitionSchema,
            Dictionary<string, KuduPredicate> predicates,
            ReadOnlySpan<byte> lowerBoundPrimaryKey,
            ReadOnlySpan<byte> upperBoundPrimaryKey,
            ReadOnlySpan<byte> lowerBoundPartitionKey,
            ReadOnlySpan<byte> upperBoundPartitionKey)
        {
            var rangeSchema = partitionSchema.RangeSchema;

            // Check if the scan can be short circuited entirely by checking the primary
            // key bounds and predicates. This also allows us to assume some invariants of the
            // scan, such as no None predicates and that the lower bound PK < upper bound PK.
            if (upperBoundPrimaryKey.Length > 0 &&
                lowerBoundPrimaryKey.SequenceCompareTo(upperBoundPrimaryKey) >= 0)
            {
                return Empty;
            }

            foreach (var predicate in predicates.Values)
            {
                if (predicate.Type == PredicateType.None)
                    return Empty;
            }

            // Build a set of partition key ranges which cover the tablets necessary for
            // the scan.
            //
            // Example predicate sets and resulting partition key ranges, based on the
            // following tablet schema:
            //
            // CREATE TABLE t (a INT32, b INT32, c INT32) PRIMARY KEY (a, b, c)
            // DISTRIBUTE BY RANGE (c)
            //               HASH (a) INTO 2 BUCKETS
            //               HASH (b) INTO 3 BUCKETS;
            //
            // Assume that hash(0) = 0 and hash(2) = 2.
            //
            // | Predicates | Partition Key Ranges                                   |
            // +------------+--------------------------------------------------------+
            // | a = 0      | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
            // | b = 2      |                                                        |
            // | c = 0      |                                                        |
            // +------------+--------------------------------------------------------+
            // | a = 0      | [(bucket=0, bucket=2), (bucket=0, bucket=3))           |
            // | b = 2      |                                                        |
            // +------------+--------------------------------------------------------+
            // | a = 0      | [(bucket=0, bucket=0, c=0), (bucket=0, bucket=0, c=1)) |
            // | c = 0      | [(bucket=0, bucket=1, c=0), (bucket=0, bucket=1, c=1)) |
            // |            | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
            // +------------+--------------------------------------------------------+
            // | b = 2      | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
            // | c = 0      | [(bucket=1, bucket=2, c=0), (bucket=1, bucket=2, c=1)) |
            // +------------+--------------------------------------------------------+
            // | a = 0      | [(bucket=0), (bucket=1))                               |
            // +------------+--------------------------------------------------------+
            // | b = 2      | [(bucket=0, bucket=2), (bucket=0, bucket=3))           |
            // |            | [(bucket=1, bucket=2), (bucket=1, bucket=3))           |
            // +------------+--------------------------------------------------------+
            // | c = 0      | [(bucket=0, bucket=0, c=0), (bucket=0, bucket=0, c=1)) |
            // |            | [(bucket=0, bucket=1, c=0), (bucket=0, bucket=1, c=1)) |
            // |            | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
            // |            | [(bucket=1, bucket=0, c=0), (bucket=1, bucket=0, c=1)) |
            // |            | [(bucket=1, bucket=1, c=0), (bucket=1, bucket=1, c=1)) |
            // |            | [(bucket=1, bucket=2, c=0), (bucket=1, bucket=2, c=1)) |
            // +------------+--------------------------------------------------------+
            // | None       | [(), ())                                               |
            //
            // If the partition key is considered as a sequence of the hash bucket
            // components and a range component, then a few patterns emerge from the
            // examples above:
            //
            // 1) The partition keys are truncated after the final constrained component
            //    Hash bucket components are constrained when the scan is limited to a
            //    subset of buckets via equality or in-list predicates on that component.
            //    Range components are constrained if they have an upper or lower bound
            //    via range or equality predicates on that component.
            //
            // 2) If the final constrained component is a hash bucket, then the
            //    corresponding bucket in the upper bound is incremented in order to make
            //    it an exclusive key.
            //
            // 3) The number of partition key ranges in the result is equal to the product
            //    of the number of buckets of each unconstrained hash component which come
            //    before a final constrained component. If there are no unconstrained hash
            //    components, then the number of resulting partition key ranges is one. Note
            //    that this can be a lot of ranges, and we may find we need to limit the
            //    algorithm to give up on pruning if the number of ranges exceeds a limit.
            //    Until this becomes a problem in practice, we'll continue always pruning,
            //    since it is precisely these highly-hash-partitioned tables which get the
            //    most benefit from pruning.

            // Step 1: Build the range portion of the partition key. If the range partition
            // columns match the primary key columns, then we can substitute the primary
            // key bounds, if they are tighter.
            ReadOnlySpan<byte> rangeLowerBound = PushPredsIntoLowerBoundRangeKey(schema, rangeSchema, predicates);
            ReadOnlySpan<byte> rangeUpperBound = PushPredsIntoUpperBoundRangeKey(schema, rangeSchema, predicates);

            if (partitionSchema.IsSimpleRangePartitioning)
            {
                if (rangeLowerBound.SequenceCompareTo(lowerBoundPrimaryKey) < 0)
                {
                    rangeLowerBound = lowerBoundPrimaryKey;
                }
                if (upperBoundPrimaryKey.Length > 0 &&
                    (rangeUpperBound.Length == 0 ||
                    rangeUpperBound.SequenceCompareTo(upperBoundPrimaryKey) > 0))
                {
                    rangeUpperBound = upperBoundPrimaryKey;
                }
            }

            // Step 2: Create the hash bucket portion of the partition key.

            // List of pruned hash buckets per hash component.
            var hashComponents = new List<BitArray>(partitionSchema.HashBucketSchemas.Count);
            foreach (HashBucketSchema hashSchema in partitionSchema.HashBucketSchemas)
            {
                hashComponents.Add(PruneHashComponent(schema, hashSchema, predicates));
            }

            // The index of the final constrained component in the partition key.
            int constrainedIndex = 0;
            if (rangeLowerBound.Length > 0 || rangeUpperBound.Length > 0)
            {
                // The range component is constrained if either of the range bounds are
                // specified (non-empty).
                constrainedIndex = partitionSchema.HashBucketSchemas.Count;
            }
            else
            {
                // Search the hash bucket constraints from right to left, looking for the
                // first constrained component.
                for (int i = hashComponents.Count; i > 0; i--)
                {
                    int numBuckets = partitionSchema.HashBucketSchemas[i - 1].NumBuckets;
                    BitArray hashBuckets = hashComponents[i - 1];
                    if (hashBuckets.NextClearBit(0) < numBuckets)
                    {
                        constrainedIndex = i;
                        break;
                    }
                }
            }

            // Build up a set of partition key ranges out of the hash components.
            //
            // Each hash component simply appends its bucket number to the
            // partition key ranges (possibly incrementing the upper bound by one bucket
            // number if this is the final constraint, see note 2 in the example above).
            var partitionKeyRanges = new List<PartitionKeyRangeBuilder> { new PartitionKeyRangeBuilder(128) };

            for (int hashIdx = 0; hashIdx < constrainedIndex; hashIdx++)
            {
                // This is the final partition key component if this is the final constrained
                // bucket, and the range upper bound is empty. In this case we need to
                // increment the bucket on the upper bound to convert from inclusive to
                // exclusive.
                bool isLast = hashIdx + 1 == constrainedIndex && rangeUpperBound.Length == 0;
                BitArray hashBuckets = hashComponents[hashIdx];

                var newPartitionKeyRanges = new List<PartitionKeyRangeBuilder>(
                    partitionKeyRanges.Count * hashBuckets.Cardinality());

                foreach (var partitionKeyRange in partitionKeyRanges)
                {
                    for (int bucket = hashBuckets.NextSetBit(0);
                         bucket != -1;
                         bucket = hashBuckets.NextSetBit(bucket + 1))
                    {
                        int bucketUpper = isLast ? bucket + 1 : bucket;
                        ArrayBufferWriter<byte> lower = partitionKeyRange.Lower.Clone();
                        ArrayBufferWriter<byte> upper = partitionKeyRange.Lower.Clone();
                        KeyEncoder.EncodeHashBucket(bucket, lower);
                        KeyEncoder.EncodeHashBucket(bucketUpper, upper);
                        newPartitionKeyRanges.Add(new PartitionKeyRangeBuilder(lower, upper));
                    }
                }
                partitionKeyRanges = newPartitionKeyRanges;
            }

            // Step 3: append the (possibly empty) range bounds to the partition key ranges.
            foreach (PartitionKeyRangeBuilder range in partitionKeyRanges)
            {
                range.Lower.Write(rangeLowerBound);
                range.Upper.Write(rangeUpperBound);
            }

            // Step 4: Filter ranges that fall outside the scan's upper and lower bound partition keys.
            var partitionKeyRangeBytes = new List<PartitionKeyRange>(partitionKeyRanges.Count);
            foreach (PartitionKeyRangeBuilder range in partitionKeyRanges)
            {
                ReadOnlySpan<byte> lower = range.Lower.WrittenSpan;
                ReadOnlySpan<byte> upper = range.Upper.WrittenSpan;

                // Sanity check that the lower bound is less than the upper bound.
                //assert upper.length == 0 || Bytes.memcmp(lower, upper) < 0;

                // Find the intersection of the ranges.
                if (lowerBoundPartitionKey.Length > 0 &&
                    (lower.Length == 0 || lower.SequenceCompareTo(lowerBoundPartitionKey) < 0))
                {
                    lower = lowerBoundPartitionKey;
                }
                if (upperBoundPartitionKey.Length > 0 &&
                    (upper.Length == 0 || upper.SequenceCompareTo(upperBoundPartitionKey) > 0))
                {
                    upper = upperBoundPartitionKey;
                }

                // If the intersection is valid, then add it as a range partition.
                if (upper.Length == 0 || lower.SequenceCompareTo(upper) < 0)
                {
                    partitionKeyRangeBytes.Add(
                        new PartitionKeyRange(lower.ToArray(), upper.ToArray()));
                }
            }

            partitionKeyRangeBytes.Reverse();

            return new PartitionPruner(new Stack<PartitionKeyRange>(partitionKeyRangeBytes));
        }

        /// <summary>
        /// True if there are more range partitions to scan.
        /// </summary>
        public bool HasMorePartitionKeyRanges => _rangePartitions.Count > 0;

        /// <summary>
        /// The number of remaining partition ranges for the scan.
        /// </summary>
        public int NumRangesRemaining => _rangePartitions.Count;

        /// <summary>
        /// The inclusive lower bound partition key of the next tablet to scan.
        /// </summary>
        public byte[] NextPartitionKey => _rangePartitions.Peek().Lower;

        /// <summary>
        /// The next range partition key range to scan.
        /// </summary>
        public PartitionKeyRange NextPartitionKeyRange => _rangePartitions.Peek();

        /// <summary>
        /// Removes all partition key ranges through the provided exclusive upper bound.
        /// </summary>
        /// <param name="upperBound">The exclusive upper bound.</param>
        public void RemovePartitionKeyRange(byte[] upperBound)
        {
            if (upperBound.Length == 0)
            {
                _rangePartitions.Clear();
                return;
            }

            while (_rangePartitions.Count > 0)
            {
                PartitionKeyRange range = _rangePartitions.Peek();
                if (upperBound.SequenceCompareTo(range.Lower) <= 0)
                {
                    break;
                }
                _rangePartitions.Pop();
                if (range.Upper.Length == 0 || upperBound.SequenceCompareTo(range.Upper) < 0)
                {
                    // The upper bound falls in the middle of this range, so add it back
                    // with the restricted bounds.
                    _rangePartitions.Push(new PartitionKeyRange(upperBound, range.Upper));
                    break;
                }
            }
        }

        internal bool ShouldPruneForTests(Partition partition)
        {
            // The C++ version uses binary search to do this with fewer key comparisons,
            // but the algorithm isn't easily translatable, so this just uses a linear
            // search.
            foreach (var range in _rangePartitions)
            {
                // Continue searching the list of ranges if the partition is greater than
                // the current range.
                if (range.Upper.Length > 0 &&
                    range.Upper.SequenceCompareTo(partition.PartitionKeyStart) <= 0)
                {
                    continue;
                }

                // If the current range is greater than the partitions,
                // then the partition should be pruned.
                return partition.PartitionKeyEnd.Length > 0 &&
                       partition.PartitionKeyEnd.SequenceCompareTo(range.Lower) <= 0;
            }

            // The partition is greater than all ranges.
            return true;
        }

        private static List<int> IdsToIndexes(KuduSchema schema, List<int> ids)
        {
            var indexes = new List<int>(ids.Count);

            foreach (int id in ids)
                indexes.Add(schema.GetColumnIndex(id));

            return indexes;
        }

        private static bool IncrementKey(PartialRow row, List<int> keyIndexes)
        {
            for (int i = keyIndexes.Count - 1; i >= 0; i--)
            {
                if (row.IncrementColumn(keyIndexes[i]))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Translates column predicates into a lower bound range partition key.
        /// </summary>
        /// <param name="schema">The table schema.</param>
        /// <param name="rangeSchema">The range partition schema.</param>
        /// <param name="predicates">The predicates.</param>
        private static byte[] PushPredsIntoLowerBoundRangeKey(
            KuduSchema schema, RangeSchema rangeSchema, Dictionary<string, KuduPredicate> predicates)
        {
            var row = new PartialRow(schema);
            int pushedPredicates = 0;

            List<int> rangePartitionColumnIdxs = IdsToIndexes(schema, rangeSchema.ColumnIds);

            // Copy predicates into the row in range partition key column order,
            // stopping after the first missing predicate.

            foreach (int idx in rangePartitionColumnIdxs)
            {
                ColumnSchema column = schema.GetColumn(idx);

                if (!predicates.TryGetValue(column.Name, out KuduPredicate predicate))
                    break;

                PredicateType predicateType = predicate.Type;

                if ((predicateType == PredicateType.Range && predicate.Lower == null) ||
                    predicateType == PredicateType.IsNotNull)
                {
                    break;
                }

                if (predicateType == PredicateType.Range ||
                    predicateType == PredicateType.Equality)
                {
                    row.SetRaw(idx, predicate.Lower);
                    pushedPredicates++;
                }
                else if (predicateType == PredicateType.InList)
                {
                    row.SetRaw(idx, predicate.InListValues.Min);
                    pushedPredicates++;
                }
                else
                {
                    throw new ArgumentException(
                        $"Unexpected predicate type can not be pushed into key: {predicate}");
                }
            }

            // If no predicates were pushed, no need to do any more work.
            if (pushedPredicates == 0)
            {
                return Array.Empty<byte>();
            }

            // For each remaining column in the partition key, fill it with the minimum value.
            for (int i = pushedPredicates; i < rangePartitionColumnIdxs.Count; i++)
            {
                row.SetMin(rangePartitionColumnIdxs[i]);
            }

            return KeyEncoder.EncodeRangePartitionKey(row, rangeSchema);
        }

        private static byte[] PushPredsIntoUpperBoundRangeKey(
            KuduSchema schema, RangeSchema rangeSchema, Dictionary<string, KuduPredicate> predicates)
        {
            var row = new PartialRow(schema);
            int pushedPredicates = 0;
            KuduPredicate finalPredicate = null;

            List<int> rangePartitionColumnIdxs = IdsToIndexes(schema, rangeSchema.ColumnIds);

            // Step 1: copy predicates into the row in range partition key column order, stopping after
            // the first missing predicate.
            foreach (int idx in rangePartitionColumnIdxs)
            {
                ColumnSchema column = schema.GetColumn(idx);

                if (!predicates.TryGetValue(column.Name, out KuduPredicate predicate))
                    break;

                PredicateType predicateType = predicate.Type;

                if (predicateType == PredicateType.Equality)
                {
                    row.SetRaw(idx, predicate.Lower);
                    pushedPredicates++;
                    finalPredicate = predicate;
                }
                else if (predicateType == PredicateType.Range)
                {
                    if (predicate.Upper != null)
                    {
                        row.SetRaw(idx, predicate.Upper);
                        pushedPredicates++;
                        finalPredicate = predicate;
                    }

                    // After the first column with a range constraint we stop pushing
                    // constraints into the upper bound. Instead, we push minimum values
                    // to the remaining columns (below), which is the maximally tight
                    // constraint.
                    break;
                }
                else if (predicateType == PredicateType.IsNotNull)
                {
                    break;
                }
                else if (predicateType == PredicateType.InList)
                {
                    row.SetRaw(idx, predicate.InListValues.Max);
                    pushedPredicates++;
                    finalPredicate = predicate;
                }
                else
                {
                    throw new ArgumentException(
                        $"Unexpected predicate type can not be pushed into key: {predicate}");
                }
            }

            // If no predicates were pushed, no need to do any more work.
            if (pushedPredicates == 0)
            {
                return Array.Empty<byte>();
            }

            // Step 2: If the final predicate is an equality or IN-list predicate, increment the
            // key to convert it to an exclusive upper bound.
            if (finalPredicate.Type == PredicateType.Equality ||
                finalPredicate.Type == PredicateType.InList)
            {
                // If the increment fails then this bound is is not constraining the keyspace.
                if (!IncrementKey(row, rangePartitionColumnIdxs.GetRange(0, pushedPredicates)))
                {
                    return Array.Empty<byte>();
                }
            }

            // Step 3: Fill the remaining columns without predicates with the min value.
            for (int i = pushedPredicates; i < rangePartitionColumnIdxs.Count; i++)
            {
                row.SetMin(rangePartitionColumnIdxs[i]);
            }

            return KeyEncoder.EncodeRangePartitionKey(row, rangeSchema);
        }

        /// <summary>
        /// Search all combination of in-list and equality predicates for pruneable hash partitions.
        /// Returns a bitset containing false bits for hash buckets which may be pruned.
        /// </summary>
        private static BitArray PruneHashComponent(
            KuduSchema schema,
            HashBucketSchema hashSchema,
            Dictionary<string, KuduPredicate> predicates)
        {
            var hashBuckets = new BitArray(hashSchema.NumBuckets);
            List<int> columnIdxs = IdsToIndexes(schema, hashSchema.ColumnIds);

            foreach (int idx in columnIdxs)
            {
                ColumnSchema column = schema.GetColumn(idx);

                if (!predicates.TryGetValue(column.Name, out KuduPredicate predicate) ||
                    predicate.Type != PredicateType.Equality &&
                    predicate.Type != PredicateType.InList)
                {
                    hashBuckets.SetAll(true);
                    return hashBuckets;
                }
            }

            var rows = new List<PartialRow> { new PartialRow(schema) };

            foreach (int idx in columnIdxs)
            {
                var newRows = new List<PartialRow>();
                ColumnSchema column = schema.GetColumn(idx);
                KuduPredicate predicate = predicates[column.Name];
                SortedSet<byte[]> predicateValues;

                if (predicate.Type == PredicateType.Equality)
                {
                    predicateValues = new SortedSet<byte[]> { predicate.Lower };
                }
                else
                {
                    predicateValues = predicate.InListValues;
                }

                // For each of the encoded string, replicate it by the number of values in
                // equality and in-list predicate.
                foreach (PartialRow row in rows)
                {
                    foreach (byte[] predicateValue in predicateValues)
                    {
                        var newRow = new PartialRow(row);
                        newRow.SetRaw(idx, predicateValue);
                        newRows.Add(newRow);
                    }
                }

                rows = newRows;
            }

            foreach (PartialRow row in rows)
            {
                int maxSize = KeyEncoder.CalculateMaxPrimaryKeySize(row);
                int hash = KeyEncoder.GetHashBucket(row, hashSchema, maxSize);
                hashBuckets.Set(hash, true);
            }

            return hashBuckets;
        }

        private readonly struct PartitionKeyRangeBuilder
        {
            public ArrayBufferWriter<byte> Lower { get; }

            public ArrayBufferWriter<byte> Upper { get; }

            public PartitionKeyRangeBuilder(int initialCapacity)
            {
                Lower = new ArrayBufferWriter<byte>(initialCapacity);
                Upper = new ArrayBufferWriter<byte>(initialCapacity);
            }

            public PartitionKeyRangeBuilder(
                ArrayBufferWriter<byte> lower,
                ArrayBufferWriter<byte> upper)
            {
                Lower = lower;
                Upper = upper;
            }
        }
    }

    public readonly struct PartitionKeyRange
    {
        public byte[] Lower { get; }

        public byte[] Upper { get; }

        public PartitionKeyRange(byte[] lower, byte[] upper)
        {
            Lower = lower;
            Upper = upper;
        }
    }
}
