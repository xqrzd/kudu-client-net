using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Mapper;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client;

public sealed class ResultSet : IEnumerable<RowResult>
{
    private static readonly ResultSetMapper _mapper = new();

    private readonly ArrayPoolBuffer<byte>? _buffer;
    private readonly byte[]? _data;
    private readonly SidecarOffset[] _dataSidecarOffsets;
    private readonly SidecarOffset[] _varlenDataSidecarOffsets;
    private readonly SidecarOffset[] _nonNullBitmapSidecarOffsets;
    private KuduSchema? _schema;

    public long Count { get; }

    internal ResultSet(
        ArrayPoolBuffer<byte>? buffer,
        KuduSchema schema,
        long count,
        SidecarOffset[] dataSidecarOffsets,
        SidecarOffset[] varlenDataSidecarOffsets,
        SidecarOffset[] nonNullBitmapSidecarOffsets)
    {
        _buffer = buffer;
        _data = buffer?.Buffer;
        _dataSidecarOffsets = dataSidecarOffsets;
        _varlenDataSidecarOffsets = varlenDataSidecarOffsets;
        _nonNullBitmapSidecarOffsets = nonNullBitmapSidecarOffsets;
        _schema = schema;
        Count = count;
    }

    public KuduSchema Schema
    {
        get
        {
            var schema = _schema;

            if (schema is null)
                ThrowObjectDisposedException();

            return schema;
        }
    }

    public T[] MapTo<T>()
    {
        var func = _mapper.CreateDelegate<T>(Schema);
        var items = new T[(int)Count];

        for (int i = 0; i < items.Length; i++)
        {
            var item = func(this, i);
            items[i] = item;
        }

        return items;
    }

    internal void Invalidate()
    {
        _schema = null;
        _buffer?.Dispose();
    }

    internal bool GetBool(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetBool(columnIndex, rowIndex);
    }

    internal bool GetBool(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduType.Bool);
        return ReadBoolUnsafe(columnIndex, rowIndex);
    }

    internal bool? GetNullableBool(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableBool(columnIndex, rowIndex);
    }

    internal bool? GetNullableBool(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduType.Bool);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadBoolUnsafe(columnIndex, rowIndex);
    }

    private bool ReadBoolUnsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 1);
        return KuduEncoder.DecodeBoolUnsafe(_data!, offset);
    }

    internal byte GetByte(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetByte(columnIndex, rowIndex);
    }

    internal byte GetByte(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduType.Int8);
        return ReadByteUnsafe(columnIndex, rowIndex);
    }

    internal byte? GetNullableByte(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableByte(columnIndex, rowIndex);
    }

    internal byte? GetNullableByte(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduType.Int8);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadByteUnsafe(columnIndex, rowIndex);
    }

    internal sbyte GetSByte(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetSByte(columnIndex, rowIndex);
    }

    internal sbyte GetSByte(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduType.Int8);
        return (sbyte)ReadByteUnsafe(columnIndex, rowIndex);
    }

    internal sbyte? GetNullableSByte(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableSByte(columnIndex, rowIndex);
    }

    internal sbyte? GetNullableSByte(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduType.Int8);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return (sbyte)ReadByteUnsafe(columnIndex, rowIndex);
    }

    private byte ReadByteUnsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 1);
        return KuduEncoder.DecodeUInt8Unsafe(_data!, offset);
    }

    internal short GetInt16(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetInt16(columnIndex, rowIndex);
    }

    internal short GetInt16(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduType.Int16);
        return ReadInt16Unsafe(columnIndex, rowIndex);
    }

    internal short? GetNullableInt16(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableInt16(columnIndex, rowIndex);
    }

    internal short? GetNullableInt16(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduType.Int16);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadInt16Unsafe(columnIndex, rowIndex);
    }

    private short ReadInt16Unsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 2);
        return KuduEncoder.DecodeInt16Unsafe(_data!, offset);
    }

    internal int GetInt32(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetInt32(columnIndex, rowIndex);
    }

    internal int GetInt32(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);
        return ReadInt32Unsafe(columnIndex, rowIndex);
    }

    internal int? GetNullableInt32(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableInt32(columnIndex, rowIndex);
    }

    internal int? GetNullableInt32(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadInt32Unsafe(columnIndex, rowIndex);
    }

    private int ReadInt32Unsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 4);
        return KuduEncoder.DecodeInt32Unsafe(_data!, offset);
    }

    internal long GetInt64(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetInt64(columnIndex, rowIndex);
    }

    internal long GetInt64(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex,
            KuduTypeFlags.Int64 |
            KuduTypeFlags.UnixtimeMicros);

        return ReadInt64Unsafe(columnIndex, rowIndex);
    }

    internal long? GetNullableInt64(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableInt64(columnIndex, rowIndex);
    }

    internal long? GetNullableInt64(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex,
            KuduTypeFlags.Int64 |
            KuduTypeFlags.UnixtimeMicros);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadInt64Unsafe(columnIndex, rowIndex);
    }

    private long ReadInt64Unsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 8);
        return KuduEncoder.DecodeInt64Unsafe(_data!, offset);
    }

    internal DateTime GetDateTime(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetDateTime(columnIndex, rowIndex);
    }

    internal DateTime GetDateTime(int columnIndex, int rowIndex)
    {
        var columnSchema = CheckTypeNotNull(columnIndex, rowIndex,
            KuduTypeFlags.UnixtimeMicros |
            KuduTypeFlags.Date);

        return columnSchema.Type == KuduType.UnixtimeMicros
            ? ReadDateTimeUnsafe(columnIndex, rowIndex)
            : ReadDateUnsafe(columnIndex, rowIndex);
    }

    internal DateTime? GetNullableDateTime(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableDateTime(columnIndex, rowIndex);
    }

    internal DateTime? GetNullableDateTime(int columnIndex, int rowIndex)
    {
        var columnSchema = CheckType(columnIndex,
            KuduTypeFlags.UnixtimeMicros |
            KuduTypeFlags.Date);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return columnSchema.Type == KuduType.UnixtimeMicros
            ? ReadDateTimeUnsafe(columnIndex, rowIndex)
            : ReadDateUnsafe(columnIndex, rowIndex);
    }

    private DateTime ReadDateTimeUnsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 8);
        return KuduEncoder.DecodeDateTimeUnsafe(_data!, offset);
    }

    private DateTime ReadDateUnsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 4);
        return KuduEncoder.DecodeDateUnsafe(_data!, offset);
    }

    internal float GetFloat(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetFloat(columnIndex, rowIndex);
    }

    internal float GetFloat(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduType.Float);
        return ReadFloatUnsafe(columnIndex, rowIndex);
    }

    internal float? GetNullableFloat(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableFloat(columnIndex, rowIndex);
    }

    internal float? GetNullableFloat(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduType.Float);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadFloatUnsafe(columnIndex, rowIndex);
    }

    private float ReadFloatUnsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 4);
        return KuduEncoder.DecodeFloatUnsafe(_data!, offset);
    }

    internal double GetDouble(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetDouble(columnIndex, rowIndex);
    }

    internal double GetDouble(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduType.Double);
        return ReadDoubleUnsafe(columnIndex, rowIndex);
    }

    internal double? GetNullableDouble(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableDouble(columnIndex, rowIndex);
    }

    internal double? GetNullableDouble(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduType.Double);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadDoubleUnsafe(columnIndex, rowIndex);
    }

    private double ReadDoubleUnsafe(int columnIndex, int rowIndex)
    {
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, 8);
        return KuduEncoder.DecodeDoubleUnsafe(_data!, offset);
    }

    internal decimal GetDecimal(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetDecimal(columnIndex, rowIndex);
    }

    internal decimal GetDecimal(int columnIndex, int rowIndex)
    {
        var column = CheckTypeNotNull(columnIndex, rowIndex,
            KuduTypeFlags.Decimal32 |
            KuduTypeFlags.Decimal64 |
            KuduTypeFlags.Decimal128);

        return ReadDecimalUnsafe(column, columnIndex, rowIndex);
    }

    internal decimal? GetNullableDecimal(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableDecimal(columnIndex, rowIndex);
    }

    internal decimal? GetNullableDecimal(int columnIndex, int rowIndex)
    {
        var column = CheckType(columnIndex,
            KuduTypeFlags.Decimal32 |
            KuduTypeFlags.Decimal64 |
            KuduTypeFlags.Decimal128);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadDecimalUnsafe(column, columnIndex, rowIndex);
    }

    private decimal ReadDecimalUnsafe(ColumnSchema column, int columnIndex, int rowIndex)
    {
        int scale = column.TypeAttributes!.Scale.GetValueOrDefault();
        int offset = GetStartIndexUnsafe(columnIndex, rowIndex, column.Size);
        return KuduEncoder.DecodeDecimalUnsafe(_data!, offset, column.Type, scale);
    }

    internal ReadOnlySpan<byte> GetSpan(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetSpan(columnIndex, rowIndex);
    }

    internal ReadOnlySpan<byte> GetSpan(int columnIndex, int rowIndex)
    {
        var column = GetColumnSchema(columnIndex);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return default;

        if (column.IsFixedSize)
        {
            int size = column.Size;
            int offset = GetStartIndexUnsafe(columnIndex, rowIndex, size);

            return _data.AsSpan(offset, size);
        }

        return ReadBinaryUnsafe(columnIndex, rowIndex);
    }

    internal string GetString(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetString(columnIndex, rowIndex);
    }

    internal string GetString(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex,
            KuduTypeFlags.String |
            KuduTypeFlags.Varchar);

        return ReadStringUnsafe(columnIndex, rowIndex);
    }

    internal string? GetNullableString(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableString(columnIndex, rowIndex);
    }

    internal string? GetNullableString(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduTypeFlags.String | KuduTypeFlags.Varchar);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadStringUnsafe(columnIndex, rowIndex);
    }

    private string ReadStringUnsafe(int columnIndex, int rowIndex)
    {
        var (offset, length) = GetVarLenStartIndexUnsafe(columnIndex, rowIndex);
        return KuduEncoder.DecodeString(_data!, offset, length);
    }

    internal byte[] GetBinary(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetBinary(columnIndex, rowIndex);
    }

    internal byte[] GetBinary(int columnIndex, int rowIndex)
    {
        CheckTypeNotNull(columnIndex, rowIndex, KuduType.Binary);
        return ReadBinaryUnsafe(columnIndex, rowIndex).ToArray();
    }

    internal byte[]? GetNullableBinary(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return GetNullableBinary(columnIndex, rowIndex);
    }

    internal byte[]? GetNullableBinary(int columnIndex, int rowIndex)
    {
        CheckType(columnIndex, KuduType.Binary);

        if (IsNullUnsafe(columnIndex, rowIndex))
            return null;

        return ReadBinaryUnsafe(columnIndex, rowIndex).ToArray();
    }

    private ReadOnlySpan<byte> ReadBinaryUnsafe(int columnIndex, int rowIndex)
    {
        var (offset, length) = GetVarLenStartIndexUnsafe(columnIndex, rowIndex);
        return _data.AsSpan(offset, length);
    }

    internal bool IsNull(string columnName, int rowIndex)
    {
        int columnIndex = GetColumnIndex(columnName);
        return IsNullUnsafe(columnIndex, rowIndex);
    }

    internal bool IsNull(int columnIndex, int rowIndex)
    {
        int offset = _nonNullBitmapSidecarOffsets[columnIndex].Start;
        return IsNullOffsetUnsafe(offset, rowIndex);
    }

    private bool IsNullUnsafe(int columnIndex, int rowIndex)
    {
        int offset = GetOffsetUnsafe(_nonNullBitmapSidecarOffsets, columnIndex);
        return IsNullOffsetUnsafe(offset, rowIndex);
    }

    private bool IsNullOffsetUnsafe(int offset, int rowIndex)
    {
        if (offset < 0)
        {
            // This column isn't nullable.
            return false;
        }

        bool nonNull = KuduEncoder.DecodeBitUnsafe(_data!, offset, rowIndex);
        return !nonNull;
    }

    private ColumnSchema CheckType(int columnIndex, KuduType type)
    {
        var columnSchema = GetColumnSchema(columnIndex);
        KuduTypeValidation.ValidateColumnType(columnSchema, type);
        return columnSchema;
    }

    private ColumnSchema CheckType(int columnIndex, KuduTypeFlags typeFlags)
    {
        var columnSchema = GetColumnSchema(columnIndex);
        KuduTypeValidation.ValidateColumnType(columnSchema, typeFlags);
        return columnSchema;
    }

    private ColumnSchema CheckFixedLengthType(int columnIndex)
    {
        var columnSchema = GetColumnSchema(columnIndex);
        KuduTypeValidation.ValidateColumnIsFixedLengthType(columnSchema);
        return columnSchema;
    }

    private ColumnSchema CheckTypeNotNull(int columnIndex, int rowIndex, KuduType type)
    {
        var columnSchema = CheckType(columnIndex, type);
        CheckNotNullUnsafe(columnSchema, columnIndex, rowIndex);
        return columnSchema;
    }

    private ColumnSchema CheckTypeNotNull(int columnIndex, int rowIndex, KuduTypeFlags typeFlags)
    {
        var columnSchema = CheckType(columnIndex, typeFlags);
        CheckNotNullUnsafe(columnSchema, columnIndex, rowIndex);
        return columnSchema;
    }

    private void CheckNotNullUnsafe(ColumnSchema columnSchema, int columnIndex, int rowIndex)
    {
        if (IsNullUnsafe(columnIndex, rowIndex))
        {
            KuduTypeValidation.ThrowNullException(columnSchema);
        }
    }

    private ColumnSchema GetColumnSchema(int columnIndex)
    {
        return Schema.GetColumn(columnIndex);
    }

    private int GetColumnIndex(string columnName)
    {
        return Schema.GetColumnIndex(columnName);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetStartIndexUnsafe(int columnIndex, int rowIndex, int dataSize)
    {
        var offset = GetOffsetUnsafe(_dataSidecarOffsets, columnIndex);
        return offset + rowIndex * dataSize;
    }

    private (int Offset, int Length) GetVarLenStartIndexUnsafe(int columnIndex, int rowIndex)
    {
        int sidecarOffset = GetOffsetUnsafe(_varlenDataSidecarOffsets, columnIndex);
        int offset = ReadInt32Unsafe(columnIndex, rowIndex);
        int length = ReadInt32Unsafe(columnIndex, rowIndex + 1) - offset;
        int realOffset = sidecarOffset + offset;

        // Bounds are verified later.
        return (realOffset, length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetOffsetUnsafe(SidecarOffset[] source, int index)
    {
#if NET5_0_OR_GREATER
        return Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(source), index).Start;
#else
        return source[index].Start;
#endif
    }

    [DoesNotReturn]
    private static void ThrowObjectDisposedException() =>
        throw new ObjectDisposedException(nameof(ResultSet));

    public override string ToString() => $"{Count} rows";

    public Enumerator GetEnumerator() => new(this);

    IEnumerator<RowResult> IEnumerable<RowResult>.GetEnumerator() => new Enumerator(this);

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

    public struct Enumerator : IEnumerator<RowResult>, IEnumerator
    {
        private readonly ResultSet _resultSet;
        private readonly int _numRows;
        private int _index;

        internal Enumerator(ResultSet resultSet)
        {
            _resultSet = resultSet;
            _index = -1;
            _numRows = resultSet._data is null
                ? 0 // Empty projection.
                : (int)resultSet.Count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            int index = _index + 1;
            if (index < _numRows)
            {
                _index = index;
                return true;
            }

            return false;
        }

        public RowResult Current => new(_resultSet, _index);

        object IEnumerator.Current => Current;

        public void Reset() => _index = -1;

        public void Dispose() { }
    }
}
