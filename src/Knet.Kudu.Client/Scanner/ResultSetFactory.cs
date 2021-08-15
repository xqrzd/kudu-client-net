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
            if (scanResponse.ColumnarData is null)
                throw new NotImplementedException("Rowwise parser not implemented yet");

            var columnarData = scanResponse.ColumnarData;
            return CreateResultSet(message, scanSchema, columnarData);
        }

        private static ResultSet CreateResultSet(
            KuduMessage message,
            KuduSchema schema,
            ColumnarRowBlockPB columnarData)
        {
            var columns = columnarData.Columns;
            var numColumns = columns.Count;

            int[] dataSidecarOffsets;
            int[] varlenDataSidecarOffsets;
            int[] nonNullBitmapSidecarOffsets;

            if (numColumns == 0)
            {
                // Empty projection, usually used for quick row counting.
                dataSidecarOffsets = Array.Empty<int>();
                varlenDataSidecarOffsets = Array.Empty<int>();
                nonNullBitmapSidecarOffsets = Array.Empty<int>();
            }
            else
            {
                dataSidecarOffsets = new int[numColumns];
                varlenDataSidecarOffsets = new int[numColumns];
                nonNullBitmapSidecarOffsets = new int[numColumns];

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
                        nonNullBitmapSidecarOffsets[i] = -1;
                    }
                }
            }

            var messageOwner = message.TakeOwnership();

            return new ResultSet(
                messageOwner,
                schema,
                columnarData.NumRows,
                dataSidecarOffsets,
                varlenDataSidecarOffsets,
                nonNullBitmapSidecarOffsets);
        }
    }
}
