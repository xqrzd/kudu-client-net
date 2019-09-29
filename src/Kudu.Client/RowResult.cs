using System;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public readonly ref struct RowResult
    {
        private readonly ResultSet _resultSet;
        private readonly ReadOnlySpan<byte> _rowData;
        private readonly ReadOnlySpan<byte> _indirectData;

        public RowResult(
            ResultSet resultSet,
            ReadOnlySpan<byte> rowData,
            ReadOnlySpan<byte> indirectData)
        {
            _resultSet = resultSet;
            _rowData = rowData;
            _indirectData = indirectData;
        }

        public int GetInt32(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetInt32(columnIndex);
        }

        public int GetInt32(int columnIndex)
        {
            //checkValidColumn(columnIndex);
            //checkNull(columnIndex);
            //checkType(columnIndex, Type.INT32);

            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 4);

            return KuduEncoder.DecodeInt32(data);
        }

        public int? GetNullableInt32(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetNullableInt32(columnIndex);
        }

        public int? GetNullableInt32(int columnIndex)
        {
            if (IsNull(columnIndex))
                return null;

            return GetInt32(columnIndex);
        }

        public string GetString(string columnName)
        {
            int columnIndex = _resultSet.GetColumnIndex(columnName);
            return GetString(columnIndex);
        }

        public string GetString(int columnIndex)
        {
            //checkValidColumn(columnIndex);
            //checkNull(columnIndex);
            //checkType(columnIndex, Type.STRING);

            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> offsetData = _rowData.Slice(position, 8);
            ReadOnlySpan<byte> lengthData = _rowData.Slice(position + 8, 8);

            int offset = (int)KuduEncoder.DecodeInt64(offsetData);
            int length = (int)KuduEncoder.DecodeInt64(lengthData);

            ReadOnlySpan<byte> data = _indirectData.Slice(offset, length);

            return KuduEncoder.DecodeString(data);
        }

        public bool IsNull(int columnIndex)
        {
            int nullBitSetOffset = _resultSet.GetNullBitSetOffset();
            bool isNull = BitmapGet(nullBitSetOffset, columnIndex);
            return isNull;
        }

        private bool BitmapGet(int offset, int index)
        {
            return (_rowData[offset + (index / 8)] & (1 << (index % 8))) != 0;
        }
    }
}
