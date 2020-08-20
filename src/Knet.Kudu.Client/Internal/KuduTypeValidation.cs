using System;
using System.Runtime.CompilerServices;

namespace Knet.Kudu.Client.Internal
{
    public static class KuduTypeValidation
    {
        public static bool IsOfType(this KuduType type, KuduTypeFlags types)
        {
            uint typeFlag = (uint)1 << (int)type;
            return (typeFlag & (uint)types) != 0;
        }

        public static void ValidateColumnType(
            this ColumnSchema column, KuduTypeFlags types)
        {
            if (!column.Type.IsOfType(types))
                ThrowException(column, types);
        }

        public static void ValidateColumnType(
            this ColumnSchema column, KuduType type)
        {
            if (column.Type != type)
                ThrowException(column, type);
        }

        public static void ValidateColumnIsFixedLengthType(this ColumnSchema column)
        {
            if (!column.IsFixedSize)
                ThrowNotFixedLengthException(column);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowException(ColumnSchema column, KuduTypeFlags types)
        {
            throw new ArgumentException(
                $"Expected column {column} to be one of ({types})");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static T ThrowException<T>(ColumnSchema column, KuduTypeFlags types)
        {
            throw new ArgumentException(
                $"Expected column {column} to be one of ({types})");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowException(ColumnSchema column, KuduType type)
        {
            throw new ArgumentException(
                $"Expected column {column} to be of {type}");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowNullException(ColumnSchema column)
        {
            throw new Exception($"Column {column} is null");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowNotNullableException(ColumnSchema column)
        {
            throw new Exception($"Column {column} is not nullable");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowNotFixedLengthException(ColumnSchema column)
        {
            throw new Exception($"Column {column} is not a fixed length type");
        }
    }
}
