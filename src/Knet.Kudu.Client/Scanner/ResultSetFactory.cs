using System;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Scanner
{
    internal static class ResultSetFactory
    {
        public static ResultSet Create(
            KuduSchema scanSchema,
            ScanResponsePB scanResponse,
            KuduMessage message)
        {
            if (scanResponse.ColumnarData is not null)
            {
                return CreateResultSet(message, scanSchema, scanResponse.ColumnarData);
            }

            return CreateResultSet(scanSchema, scanResponse.Data);
        }

        private static ResultSet CreateResultSet(
            KuduMessage message,
            KuduSchema schema,
            ColumnarRowBlockPB data)
        {
            if (data.Columns.Count == 0)
            {
                // Empty projection, usually used for quick row counting.
                return CreateEmptyResultSet(schema, data.NumRows);
            }

            var columns = data.Columns;
            var numColumns = columns.Count;

            var dataSidecarOffsets = new SidecarOffset[numColumns];
            var varlenDataSidecarOffsets = new SidecarOffset[numColumns];
            var nonNullBitmapSidecarOffsets = new SidecarOffset[numColumns];

            for (int i = 0; i < numColumns; i++)
            {
                var column = columns[i];

                if (column.HasDataSidecar)
                {
                    var offset = message.GetSidecarOffset(column.DataSidecar);
                    dataSidecarOffsets[i] = offset;
                }

                if (column.HasVarlenDataSidecar)
                {
                    var offset = message.GetSidecarOffset(column.VarlenDataSidecar);
                    varlenDataSidecarOffsets[i] = offset;
                }

                if (column.HasNonNullBitmapSidecar)
                {
                    var offset = message.GetSidecarOffset(column.NonNullBitmapSidecar);
                    nonNullBitmapSidecarOffsets[i] = offset;
                }
                else
                {
                    nonNullBitmapSidecarOffsets[i] = new SidecarOffset(-1, 0);
                }
            }

            var buffer = message.TakeMemory();

            return new ResultSet(
                buffer,
                schema,
                data.NumRows,
                dataSidecarOffsets,
                varlenDataSidecarOffsets,
                nonNullBitmapSidecarOffsets);
        }

        private static ResultSet CreateResultSet(
            KuduSchema schema,
            RowwiseRowBlockPB data)
        {
            if (data is null || data.NumRows == 0)
            {
                // Empty projection, usually used for quick row counting.
                return CreateEmptyResultSet(schema, data.NumRows);
            }

            throw new NotImplementedException("Support for row data will be implemented in a future PR");
        }

        private static ResultSet CreateEmptyResultSet(KuduSchema schema, long numRows)
        {
            return new ResultSet(
                null,
                schema,
                numRows,
                Array.Empty<SidecarOffset>(),
                Array.Empty<SidecarOffset>(),
                Array.Empty<SidecarOffset>());
        }
    }
}
