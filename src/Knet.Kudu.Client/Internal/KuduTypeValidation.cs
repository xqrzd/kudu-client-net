using System;
using System.Runtime.CompilerServices;

namespace Knet.Kudu.Client.Internal;

internal static class KuduTypeValidation
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsOfType(this KuduType type, KuduTypeFlags types)
    {
        int typeFlag = 1 << (int)type;
        return (typeFlag & (int)types) != 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ValidateColumnType(
        this ColumnSchema column, KuduTypeFlags types)
    {
        if (!column.Type.IsOfType(types))
            ThrowException(column, types);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ValidateColumnType(
        this ColumnSchema column, KuduType type)
    {
        if (column.Type != type)
            ThrowException(column, type);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ValidateColumnIsFixedLengthType(this ColumnSchema column)
    {
        if (!column.IsFixedSize)
            ThrowNotFixedLengthException(column);
    }

    public static void ThrowException(ColumnSchema column, KuduTypeFlags types)
    {
        throw new ArgumentException(
            $"Expected column {column} to be one of ({types})");
    }

    // TODO: Remove this method
    public static T ThrowException<T>(ColumnSchema column, KuduTypeFlags types)
    {
        throw new ArgumentException(
            $"Expected column {column} to be one of ({types})");
    }

    public static void ThrowException(ColumnSchema column, KuduType type)
    {
        throw new ArgumentException(
            $"Expected column {column} to be of {type}");
    }

    public static void ThrowNullException(ColumnSchema column)
    {
        throw new ArgumentException($"Column {column} is null");
    }

    public static void ThrowNotNullableException(ColumnSchema column)
    {
        throw new ArgumentException($"Column {column} is not nullable");
    }

    public static void ThrowNotFixedLengthException(ColumnSchema column)
    {
        throw new ArgumentException($"Column {column} is not a fixed length type");
    }
}
