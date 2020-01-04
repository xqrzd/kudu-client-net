using System.Collections.Generic;
using Knet.Kudu.Client.Internal;

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
        public static void Encode(
            List<KuduOperation> operations,
            BufferWriter rowAllocWriter,
            BufferWriter indirectDataWriter)
        {
            foreach (var row in operations)
            {
                var rowSpan = rowAllocWriter.GetSpan(row.RowSizeWithOperation);
                var indirectSpan = indirectDataWriter.GetSpan(row.IndirectDataSize);

                row.WriteToWithOperation(rowSpan, indirectSpan);

                rowAllocWriter.Advance(rowSpan.Length);
                indirectDataWriter.Advance(indirectSpan.Length);
            }
        }
    }
}
