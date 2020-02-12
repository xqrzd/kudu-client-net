using System;
using System.Collections.Generic;

namespace Knet.Kudu.Client
{
    public static class ScanBuilderExtensions
    {
        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, bool value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, long value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, DateTime value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, float value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, double value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, decimal value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, string value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddComparisonPredicate(
            this ScanBuilder scanBuilder, string columnName, ComparisonOp op, byte[] value)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddIsNotNullPredicate(
            this ScanBuilder scanBuilder, string columnName)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewIsNotNullPredicate(column);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddIsNullPredicate(
            this ScanBuilder scanBuilder, string columnName)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewIsNullPredicate(column);
            return scanBuilder.AddPredicate(predicate);
        }

        public static ScanBuilder AddInListPredicate<T>(
            this ScanBuilder scanBuilder,
            string columnName, IEnumerable<T> values)
        {
            var column = scanBuilder.Table.Schema.GetColumn(columnName);
            var predicate = KuduPredicate.NewInListPredicate(column, values);
            return scanBuilder.AddPredicate(predicate);
        }
    }
}
