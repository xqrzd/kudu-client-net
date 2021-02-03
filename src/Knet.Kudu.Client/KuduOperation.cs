using System;
using System.Collections.Generic;

namespace Knet.Kudu.Client
{
    public class KuduOperation : PartialRowOperation
    {
        public KuduTable Table { get; }

        public KuduOperation(KuduTable table, RowOperation operation)
            : base(table.Schema, operation)
        {
            Table = table;
        }
    }

    public static class OperationsEncoder
    {
        public static void ComputeSize<T>(
            List<T> operations,
            out int rowSize,
            out int indirectSize) where T : PartialRowOperation
        {
            int localRowSize = 0;
            int localIndirectSize = 0;

            foreach (var row in operations)
            {
                row.CalculateSize(out int rSize, out int iSize);
                localRowSize += rSize + 1; // Add 1 for RowOperation.
                localIndirectSize += iSize;
            }

            rowSize = localRowSize;
            indirectSize = localIndirectSize;
        }

        public static void Encode<T>(
            List<T> operations,
            Span<byte> rowDestination,
            Span<byte> indirectDestination) where T : PartialRowOperation
        {
            int indirectDataOffset = 0;

            foreach (var row in operations)
            {
                row.WriteToWithOperation(
                    rowDestination,
                    indirectDestination,
                    indirectDataOffset,
                    out int rowBytesWritten,
                    out int indirectBytesWritten);

                rowDestination = rowDestination.Slice(rowBytesWritten);
                indirectDestination = indirectDestination.Slice(indirectBytesWritten);

                indirectDataOffset += indirectBytesWritten;
            }
        }
    }
}
