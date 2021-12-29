using System;
using System.Diagnostics.CodeAnalysis;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Scanner;

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

        return CreateResultSet(message, scanSchema, scanResponse.Data);
    }

    private static ResultSet CreateResultSet(
        KuduMessage message,
        KuduSchema schema,
        ColumnarRowBlockPB data)
    {
        var columns = data.Columns;
        var numColumns = columns.Count;

        if (numColumns != schema.Columns.Count)
        {
            ThrowColumnCountMismatchException(schema.Columns.Count, numColumns);
        }

        if (data.Columns.Count == 0 || data.NumRows == 0)
        {
            // Empty projection, usually used for quick row counting.
            return CreateEmptyResultSet(schema, data.NumRows);
        }

        var numRows = checked((int)data.NumRows);
        var bufferLength = message.Buffer.Length;
        var nonNullBitmapLength = KuduEncoder.BitsToBytes(numRows);
        var dataSidecarOffsets = new SidecarOffset[numColumns];
        var varlenDataSidecarOffsets = new SidecarOffset[numColumns];
        var nonNullBitmapSidecarOffsets = new SidecarOffset[numColumns];

        for (int i = 0; i < numColumns; i++)
        {
            var column = columns[i];
            var columnSchema = schema.GetColumn(i);

            if (column.HasDataSidecar)
            {
                var offset = message.GetSidecarOffset(column.DataSidecar);
                var length = GetColumnDataSize(columnSchema, numRows);
                ValidateSidecar(offset, length, bufferLength);
                dataSidecarOffsets[i] = offset;
            }
            else
            {
                ThrowMissingDataSidecarException(columnSchema);
            }

            if (column.HasVarlenDataSidecar)
            {
                var offset = message.GetSidecarOffset(column.VarlenDataSidecar);
                varlenDataSidecarOffsets[i] = offset;
            }

            if (column.HasNonNullBitmapSidecar)
            {
                var offset = message.GetSidecarOffset(column.NonNullBitmapSidecar);
                ValidateSidecar(offset, nonNullBitmapLength, bufferLength);
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
        KuduMessage message,
        KuduSchema schema,
        RowwiseRowBlockPB data)
    {
        if (data is null)
        {
            return CreateEmptyResultSet(schema, 0);
        }

        if (!data.HasRowsSidecar || schema.Columns.Count == 0 || data.NumRows == 0)
        {
            // Empty projection, usually used for quick row counting.
            return CreateEmptyResultSet(schema, data.NumRows);
        }

        return RowwiseResultSetConverter.Convert(message, schema, data);
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

    /// <summary>
    /// Retrieves the expected size of the DataSidecar for the given column
    /// and number of rows.
    /// </summary>
    private static int GetColumnDataSize(ColumnSchema column, int numRows)
    {
        // For var-length data types, DataSidecar stores the offset to the real
        // data in VarlenDataSidecar. The size of the data can be determined by
        // looking at the start of the next row, so we expect n + 1 offsets.
        return column.IsFixedSize
            ? numRows * column.Size
            : numRows * 4 + 4;
    }

    private static void ValidateSidecar(SidecarOffset offset, int length, int bufferLength)
    {
        var offsetStart = offset.Start;
        var endOffset = checked(offsetStart + length);
        if (offsetStart < 0 || endOffset > bufferLength)
        {
            ThrowSidecarOutsideBoundsException(offsetStart, length, bufferLength);
        }
    }

    [DoesNotReturn]
    private static void ThrowColumnCountMismatchException(int schemaColumns, int sidecarColumns)
    {
        throw new InvalidOperationException(
            $"Projected schema has {schemaColumns} columns, but the server returned {sidecarColumns} columns");
    }

    [DoesNotReturn]
    private static void ThrowMissingDataSidecarException(ColumnSchema column)
    {
        throw new InvalidOperationException($"Server didn't supply a data sidecar for {column}");
    }

    [DoesNotReturn]
    private static void ThrowSidecarOutsideBoundsException(int start, int length, int bufferSize)
    {
        throw new InvalidOperationException(
            "Sidecar offset is outside the bounds of the buffer. " +
            $"Start: {start}, Length: {length}, Buffer size: {bufferSize}");
    }
}
