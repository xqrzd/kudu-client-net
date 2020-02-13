using System;
using System.Collections.Generic;

namespace Knet.Kudu.Client
{
    public static class ScanBuilderExtensions
    {
        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, bool value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, long value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, DateTime value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, float value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, double value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, decimal value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, string value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddComparisonPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, ComparisonOp op, byte[] value)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddIsNotNullPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder, string columnName)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewIsNotNullPredicate(column);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddIsNullPredicate<TBuilder, TOutput>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder, string columnName)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewIsNullPredicate(column);
            return scanBuilder.AddPredicate(predicate);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddInListPredicate<TBuilder, TOutput, T>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, params T[] values)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            return AddInListPredicate(scanBuilder, columnName, (IEnumerable<T>)values);
        }

        public static AbstractKuduScannerBuilder<TBuilder, TOutput> AddInListPredicate<TBuilder, TOutput, T>(
            this AbstractKuduScannerBuilder<TBuilder, TOutput> scanBuilder,
            string columnName, IEnumerable<T> values)
            where TBuilder : AbstractKuduScannerBuilder<TBuilder, TOutput>
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewInListPredicate(column, values);
            return scanBuilder.AddPredicate(predicate);
        }
    }
}
