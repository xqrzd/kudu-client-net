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

        public int GetInt32(int columnIndex)
        {
            //checkValidColumn(columnIndex);
            //checkNull(columnIndex);
            //checkType(columnIndex, Type.INT32);

            int position = _resultSet.GetOffset(columnIndex);
            ReadOnlySpan<byte> data = _rowData.Slice(position, 4);

            return KuduEncoder.DecodeInt32(data);
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
    }
}
