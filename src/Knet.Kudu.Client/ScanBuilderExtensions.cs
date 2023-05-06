using System;
using System.Collections.Generic;
using System.Linq;
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
        this TBuilder scanBuilder, string columnName, params T[] values)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        return AddInListPredicate(scanBuilder, columnName, (IEnumerable<T>)values);
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
        this TBuilder scanBuilder, params KuduBloomFilter[] bloomFilter)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var predicate = KuduPredicate.NewInBloomFilterPredicate(bloomFilter.ToList());
        return scanBuilder.AddPredicate(predicate);
    }

    public static TBuilder AddInBloomFilterPredicate<TBuilder>(
        this TBuilder scanBuilder, IEnumerable<KuduBloomFilter> bloomFilter)
        where TBuilder : AbstractKuduScannerBuilder<TBuilder>
    {
        var predicate = KuduPredicate.NewInBloomFilterPredicate(bloomFilter.AsList());
        return scanBuilder.AddPredicate(predicate);
    }
}
