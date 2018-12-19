using System.Collections.Generic;
using Kudu.Client.Internal;

namespace Kudu.Client
{
    public struct Operation
    {
        public KuduTable Table { get; }

        public PartialRow Row { get; }

        // TODO: Should this store ChangeType instead of PartialRow?

        public Operation(KuduTable table, PartialRow row)
        {
            Table = table;
            Row = row;
        }
    }

    public static class OperationsEncoder
    {
        public static void Encode(
            List<Operation> operations,
            BufferWriter rowAllocWriter,
            BufferWriter indirectDataWriter)
        {
            foreach (var operation in operations)
            {
                var row = operation.Row;
                var rowSpan = rowAllocWriter.GetSpan(row.RowSize);
                var indirectSpan = indirectDataWriter.GetSpan(row.IndirectDataSize);

                row.WriteTo(rowSpan, indirectSpan);

                rowAllocWriter.Advance(rowSpan.Length);
                indirectDataWriter.Advance(indirectSpan.Length);
            }
        }
    }
}
