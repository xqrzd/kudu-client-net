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
            int rSize = 0;
            int iSize = 0;

            foreach (var row in operations)
            {
                // TODO: Combine these 2 calls into one method.
                rSize += row.RowSizeWithOperation;
                iSize += row.IndirectDataSize;
            }

            rowSize = rSize;
            indirectSize = iSize;
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
