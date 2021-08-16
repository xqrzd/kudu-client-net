using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public sealed class ResultSet : IEnumerable<RowResult>, IDisposable
    {
        private readonly ArrayPoolBuffer<byte> _buffer;
        private readonly byte[] _data;
        private readonly int[] _dataSidecarOffsets;
        private readonly int[] _varlenDataSidecarOffsets;
        private readonly int[] _nonNullBitmapSidecarOffsets;

        public KuduSchema Schema { get; }

        public long Count { get; }

        internal ResultSet(
            ArrayPoolBuffer<byte> buffer,
            KuduSchema schema,
            long count,
            int[] dataSidecarOffsets,
            int[] varlenDataSidecarOffsets,
            int[] nonNullBitmapSidecarOffsets)
        {
            _buffer = buffer;
            _data = buffer?.Buffer;
            _dataSidecarOffsets = dataSidecarOffsets;
            _varlenDataSidecarOffsets = varlenDataSidecarOffsets;
            _nonNullBitmapSidecarOffsets = nonNullBitmapSidecarOffsets;

            Schema = schema;
            Count = count;
        }

        public void Dispose()
        {
            _buffer?.Dispose();
        }

        internal int GetDataOffset(int columnIndex) =>
            _dataSidecarOffsets[columnIndex];

        internal int GetVarLenOffset(int columnIndex) =>
            _varlenDataSidecarOffsets[columnIndex];

        internal int GetNonNullBitmapOffset(int columnIndex) =>
            _nonNullBitmapSidecarOffsets[columnIndex];

        internal bool GetBool(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetBool(columnIndex, rowIndex);
        }

        internal bool GetBool(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex, KuduType.Bool);
            return ReadBool(columnIndex, rowIndex);
        }

        internal bool? GetNullableBool(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableBool(columnIndex, rowIndex);
        }

        internal bool? GetNullableBool(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduType.Bool);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadBool(columnIndex, rowIndex);
        }

        private bool ReadBool(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 1);
            return KuduEncoder.DecodeBool(_data, offset);
        }

        internal byte GetByte(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetByte(columnIndex, rowIndex);
        }

        internal byte GetByte(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex, KuduType.Int8);
            return ReadByte(columnIndex, rowIndex);
        }

        internal byte? GetNullableByte(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableByte(columnIndex, rowIndex);
        }

        internal byte? GetNullableByte(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduType.Int8);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadByte(columnIndex, rowIndex);
        }

        internal sbyte GetSByte(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetSByte(columnIndex, rowIndex);
        }

        internal sbyte GetSByte(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex, KuduType.Int8);
            return (sbyte)ReadByte(columnIndex, rowIndex);
        }

        internal sbyte? GetNullableSByte(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableSByte(columnIndex, rowIndex);
        }

        internal sbyte? GetNullableSByte(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduType.Int8);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return (sbyte)ReadByte(columnIndex, rowIndex);
        }

        private byte ReadByte(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 1);
            return KuduEncoder.DecodeUInt8(_data, offset);
        }

        internal short GetInt16(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetInt16(columnIndex, rowIndex);
        }

        internal short GetInt16(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex, KuduType.Int16);
            return ReadInt16(columnIndex, rowIndex);
        }

        internal short? GetNullableInt16(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableInt16(columnIndex, rowIndex);
        }

        internal short? GetNullableInt16(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduType.Int16);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadInt16(columnIndex, rowIndex);
        }

        private short ReadInt16(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 2);
            return KuduEncoder.DecodeInt16(_data, offset);
        }

        internal int GetInt32(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetInt32(columnIndex, rowIndex);
        }

        internal int GetInt32(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);
            return ReadInt32(columnIndex, rowIndex);
        }

        internal int? GetNullableInt32(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableInt32(columnIndex, rowIndex);
        }

        internal int? GetNullableInt32(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduTypeFlags.Int32 | KuduTypeFlags.Date);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadInt32(columnIndex, rowIndex);
        }

        private int ReadInt32(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 4);
            return KuduEncoder.DecodeInt32(_data, offset);
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

            return ReadInt64(columnIndex, rowIndex);
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

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadInt64(columnIndex, rowIndex);
        }

        private long ReadInt64(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 8);
            return KuduEncoder.DecodeInt64(_data, offset);
        }

        internal DateTime GetDateTime(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetDateTime(columnIndex, rowIndex);
        }

        internal DateTime GetDateTime(int columnIndex, int rowIndex)
        {
            var columnSchema = GetColumnSchema(columnIndex);
            var type = columnSchema.Type;

            CheckNotNull(columnSchema, columnIndex, rowIndex);

            if (type == KuduType.UnixtimeMicros)
            {
                return ReadDateTime(columnIndex, rowIndex);
            }
            else if (type == KuduType.Date)
            {
                return ReadDate(columnIndex, rowIndex);
            }

            return KuduTypeValidation.ThrowException<DateTime>(columnSchema,
                KuduTypeFlags.UnixtimeMicros |
                KuduTypeFlags.Date);
        }

        internal DateTime? GetNullableDateTime(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableDateTime(columnIndex, rowIndex);
        }

        internal DateTime? GetNullableDateTime(int columnIndex, int rowIndex)
        {
            var columnSchema = GetColumnSchema(columnIndex);
            var type = columnSchema.Type;

            if (IsNull(columnIndex, rowIndex))
                return null;

            if (type == KuduType.UnixtimeMicros)
            {
                return ReadDateTime(columnIndex, rowIndex);
            }
            else if (type == KuduType.Date)
            {
                return ReadDate(columnIndex, rowIndex);
            }

            return KuduTypeValidation.ThrowException<DateTime>(columnSchema,
                KuduTypeFlags.UnixtimeMicros |
                KuduTypeFlags.Date);
        }

        private DateTime ReadDateTime(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 8);
            return KuduEncoder.DecodeDateTime(_data, offset);
        }

        private DateTime ReadDate(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 4);
            return KuduEncoder.DecodeDate(_data, offset);
        }

        internal float GetFloat(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetFloat(columnIndex, rowIndex);
        }

        internal float GetFloat(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex, KuduType.Float);
            return ReadFloat(columnIndex, rowIndex);
        }

        internal float? GetNullableFloat(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableFloat(columnIndex, rowIndex);
        }

        internal float? GetNullableFloat(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduType.Float);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadFloat(columnIndex, rowIndex);
        }

        private float ReadFloat(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 4);
            return KuduEncoder.DecodeFloat(_data, offset);
        }

        internal double GetDouble(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetDouble(columnIndex, rowIndex);
        }

        internal double GetDouble(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex, KuduType.Double);
            return ReadDouble(columnIndex, rowIndex);
        }

        internal double? GetNullableDouble(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableDouble(columnIndex, rowIndex);
        }

        internal double? GetNullableDouble(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduType.Double);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadDouble(columnIndex, rowIndex);
        }

        private double ReadDouble(int columnIndex, int rowIndex)
        {
            int offset = GetStartIndex(columnIndex, rowIndex, 8);
            return KuduEncoder.DecodeDouble(_data, offset);
        }

        internal decimal GetDecimal(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetDecimal(columnIndex, rowIndex);
        }

        internal decimal GetDecimal(int columnIndex, int rowIndex)
        {
            CheckTypeNotNull(columnIndex, rowIndex,
                KuduTypeFlags.Decimal32 |
                KuduTypeFlags.Decimal64 |
                KuduTypeFlags.Decimal128);

            return ReadDecimal(columnIndex, rowIndex);
        }

        internal decimal? GetNullableDecimal(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableDecimal(columnIndex, rowIndex);
        }

        internal decimal? GetNullableDecimal(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex,
                KuduTypeFlags.Decimal32 |
                KuduTypeFlags.Decimal64 |
                KuduTypeFlags.Decimal128);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadDecimal(columnIndex, rowIndex);
        }

        private decimal ReadDecimal(int columnIndex, int rowIndex)
        {
            var column = GetColumnSchema(columnIndex);
            int scale = column.TypeAttributes.Scale.GetValueOrDefault();

            int offset = GetStartIndex(columnIndex, rowIndex, column.Size);
            return KuduEncoder.DecodeDecimal(_data, offset, column.Type, scale);
        }

        internal ReadOnlySpan<byte> GetRawFixed(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetRawFixed(columnIndex, rowIndex);
        }

        internal ReadOnlySpan<byte> GetRawFixed(int columnIndex, int rowIndex)
        {
            var column = CheckFixedLengthType(columnIndex);

            if (IsNull(columnIndex, rowIndex))
                return default;

            int size = column.Size;
            int offset = GetStartIndex(columnIndex, rowIndex, size);

            return _data.AsSpan(offset, size);
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

            return ReadString(columnIndex, rowIndex);
        }

        internal string GetNullableString(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetNullableString(columnIndex, rowIndex);
        }

        internal string GetNullableString(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduTypeFlags.String | KuduTypeFlags.Varchar);

            if (IsNull(columnIndex, rowIndex))
                return null;

            return ReadString(columnIndex, rowIndex);
        }

        private string ReadString(int columnIndex, int rowIndex)
        {
            int offset = ReadInt32(columnIndex, rowIndex);
            int length = ReadInt32(columnIndex, rowIndex + 1) - offset;

            int sidecarOffset = GetVarLenOffset(columnIndex);
            int realOffset = sidecarOffset + offset;

            return KuduEncoder.DecodeString(_data, realOffset, length);
        }

        internal ReadOnlySpan<byte> GetBinary(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return GetBinary(columnIndex, rowIndex);
        }

        internal ReadOnlySpan<byte> GetBinary(int columnIndex, int rowIndex)
        {
            CheckType(columnIndex, KuduType.Binary);

            if (IsNull(columnIndex, rowIndex))
                return default;

            return ReadBinary(columnIndex, rowIndex);
        }

        private ReadOnlySpan<byte> ReadBinary(int columnIndex, int rowIndex)
        {
            int offset = ReadInt32(columnIndex, rowIndex);
            int length = ReadInt32(columnIndex, rowIndex + 1) - offset;

            int sidecarOffset = GetVarLenOffset(columnIndex);
            int realOffset = sidecarOffset + offset;

            return _data.AsSpan(realOffset, length);
        }

        internal bool IsNull(string columnName, int rowIndex)
        {
            int columnIndex = GetColumnIndex(columnName);
            return IsNull(columnIndex, rowIndex);
        }

        internal bool IsNull(int columnIndex, int rowIndex)
        {
            int offset = GetNonNullBitmapOffset(columnIndex);
            if (offset == -1)
            {
                // This column isn't nullable.
                return false;
            }

            bool valueExists = _data.GetBit(offset, rowIndex);
            return !valueExists;
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
            CheckNotNull(columnSchema, columnIndex, rowIndex);
            return columnSchema;
        }

        private ColumnSchema CheckTypeNotNull(int columnIndex, int rowIndex, KuduTypeFlags typeFlags)
        {
            var columnSchema = CheckType(columnIndex, typeFlags);
            CheckNotNull(columnSchema, columnIndex, rowIndex);
            return columnSchema;
        }

        private void CheckNotNull(ColumnSchema columnSchema, int columnIndex, int rowIndex)
        {
            if (IsNull(columnIndex, rowIndex))
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
        private int GetStartIndex(int columnIndex, int rowIndex, int dataSize)
        {
            return GetDataOffset(columnIndex) + rowIndex * dataSize;
        }

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
                _numRows = resultSet._dataSidecarOffsets is null
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

            public void Reset() { }

            public void Dispose() { }
        }
    }
}
