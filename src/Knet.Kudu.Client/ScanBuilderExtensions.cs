using System;
using System.Collections.Generic;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client;

public static class ScanBuilderExtensions
{
    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, bool value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, long value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, DateTime value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, float value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, double value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, decimal value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, string value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddComparisonPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName, ComparisonOp op, byte[] value)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewComparisonPredicate(column, op, value);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddIsNotNullPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewIsNotNullPredicate(column);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddIsNullPredicate<TBuilder>(
        this TBuilder scanBuilder, string columnName)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewIsNullPredicate(column);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddInListPredicate<TBuilder, T>(
        this TBuilder scanBuilder, string columnName, IEnumerable<T> values)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var column = scanBuilder.Table.Schema.GetColumn(columnName);
        var predicate = KuduPredicate.NewInListPredicate(column, values);
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddInBloomFilterPredicate<TBuilder>(
        this TBuilder scanBuilder, KuduBloomFilter bloomFilter)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        return AddInBloomFilterPredicate(
            scanBuilder, new List<KuduBloomFilter> { bloomFilter });
    }

    public static TBuilder AddInBloomFilterPredicate<TBuilder>(
        this TBuilder scanBuilder, IEnumerable<KuduBloomFilter> bloomFilters)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var predicate = KuduPredicate.NewInBloomFilterPredicate(bloomFilters.AsList());
        return scanBuilder.AddPredicate(predicate);
    }
}
