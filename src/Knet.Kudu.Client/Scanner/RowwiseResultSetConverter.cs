using System;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Scanner;

internal static class RowwiseResultSetConverter
{
    // Used to convert the rowwise data to the newer columnar format,
    // to avoid virtual calls on ResultSet.
    // This is only used if the Kudu server is 1.11 or older.
    public static ResultSet Convert(
        KuduMessage message,
        KuduSchema schema,
        RowwiseRowBlockPB rowPb)
    {
        var numColumns = schema.Columns.Count;
        int columnOffsetsSize = numColumns;
        if (schema.HasNullableColumns)
        {
            columnOffsetsSize++;
        }

        var columnOffsets = new int[columnOffsetsSize];

        int currentOffset = 0;
        columnOffsets[0] = currentOffset;
        // Pre-compute the columns offsets in rowData for easier lookups later.
        // If the schema has nullables, we also add the offset for the null bitmap at the end.
        for (int i = 1; i < columnOffsetsSize; i++)
        {
            ColumnSchema column = schema.GetColumn(i - 1);
            int previousSize = column.Size;
            columnOffsets[i] = previousSize + currentOffset;
            currentOffset += previousSize;
        }

        var rowData = GetRowData(message, rowPb);
        var indirectData = GetIndirectData(message, rowPb);
        int nonNullBitmapOffset = columnOffsets[columnOffsets.Length - 1];
        int rowSize = schema.RowSize;

        int numRows = rowPb.NumRows;
        var dataSidecarOffsets = new SidecarOffset[numColumns];
        var varlenDataSidecarOffsets = new SidecarOffset[numColumns];
        var nonNullBitmapSidecarOffsets = new SidecarOffset[numColumns];
        int nonNullBitmapSize = KuduEncoder.BitsToBytes(numRows);
        int offset = 0;

        for (int i = 0; i < numColumns; i++)
        {
            var column = schema.GetColumn(i);
            var dataSize = column.IsFixedSize
                ? column.Size * numRows
                : (4 * numRows) + 4;

            dataSidecarOffsets[i] = new SidecarOffset(offset, dataSize);
            offset += dataSize;

            if (column.IsNullable)
            {
                nonNullBitmapSidecarOffsets[i] = new SidecarOffset(offset, nonNullBitmapSize);
                offset += nonNullBitmapSize;
            }
            else
            {
                nonNullBitmapSidecarOffsets[i] = new SidecarOffset(-1, 0);
            }
        }

        var buffer = new ArrayPoolBuffer<byte>(offset + indirectData.Length);
        var data = buffer.Buffer;
        data.AsSpan().Clear();

        var varlenData = data.AsSpan(offset);
        int currentDataOffset = 0;
        int currentVarlenOffset = 0;

        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++)
        {
            var column = schema.GetColumn(columnIndex);
            var isFixedSize = column.IsFixedSize;
            var columnarSize = isFixedSize ? column.Size : 4;
            var rowwiseSize = column.Size;

            var dataOffset = dataSidecarOffsets[columnIndex];
            var nonNullOffset = nonNullBitmapSidecarOffsets[columnIndex].Start;
            var dataOutput = data.AsSpan(dataOffset.Start, dataOffset.Length);

            for (int rowIndex = 0; rowIndex < numRows; rowIndex++)
            {
                bool isSet = true;
                var rowSlice = rowData.Slice(rowSize * rowIndex, rowSize);

                if (nonNullOffset > 0)
                {
                    isSet = !rowSlice.GetBit(nonNullBitmapOffset, columnIndex);

                    if (isSet)
                    {
                        data.SetBit(nonNullOffset, rowIndex);
                    }
                }

                if (isSet)
                {
                    if (isFixedSize)
                    {
                        var rawData = rowSlice.Slice(currentDataOffset, columnarSize);
                        rawData.CopyTo(dataOutput);
                    }
                    else
                    {
                        var offsetData = rowSlice.Slice(currentDataOffset, 8);
                        var lengthData = rowSlice.Slice(currentDataOffset + 8, 8);
                        int start = (int)KuduEncoder.DecodeInt64(offsetData);
                        int length = (int)KuduEncoder.DecodeInt64(lengthData);

                        var indirectSlice = indirectData.Slice(start, length);
                        indirectSlice.CopyTo(varlenData);
                        varlenData = varlenData.Slice(length);

                        KuduEncoder.EncodeInt32(dataOutput, currentVarlenOffset);
                        currentVarlenOffset += length;
                    }
                }

                dataOutput = dataOutput.Slice(columnarSize);
            }

            currentDataOffset += rowwiseSize;

            if (!isFixedSize)
            {
                KuduEncoder.EncodeInt32(dataOutput, currentVarlenOffset);
                varlenDataSidecarOffsets[columnIndex] = new SidecarOffset(offset, currentVarlenOffset);

                offset += currentVarlenOffset;
                currentVarlenOffset = 0;
            }
        }

        return new ResultSet(
            buffer,
            schema,
            numRows,
            dataSidecarOffsets,
            varlenDataSidecarOffsets,
            nonNullBitmapSidecarOffsets);
    }

    private static ReadOnlySpan<byte> GetRowData(KuduMessage message, RowwiseRowBlockPB rowPb)
    {
        if (rowPb.HasRowsSidecar)
        {
            var offset = message.GetSidecarOffset(rowPb.RowsSidecar);
            return message.Buffer.AsSpan(offset.Start, offset.Length);
        }

        return default;
    }

    private static ReadOnlySpan<byte> GetIndirectData(KuduMessage message, RowwiseRowBlockPB rowPb)
    {
        if (rowPb.HasIndirectDataSidecar)
        {
            var offset = message.GetSidecarOffset(rowPb.IndirectDataSidecar);
            return message.Buffer.AsSpan(offset.Start, offset.Length);
        }

        return default;
    }
}
