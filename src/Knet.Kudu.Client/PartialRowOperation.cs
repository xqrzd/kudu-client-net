using System;

namespace Knet.Kudu.Client
{
    public class PartialRowOperation : PartialRow
    {
        internal RowOperation Operation { get; }

        public PartialRowOperation(KuduSchema schema, RowOperation operation)
            : base(schema)
        {
            Operation = operation;
        }

        internal PartialRowOperation(PartialRowOperation row, RowOperation operation)
            : base(row)
        {
            Operation = operation;
        }

        public void WriteToWithOperation(
            Span<byte> rowDestination,
            Span<byte> indirectDestination,
            int indirectDataStart,
            out int rowBytesWritten,
            out int indirectBytesWritten)
        {
            rowDestination[0] = (byte)Operation;
            rowDestination = rowDestination.Slice(1);

            WriteTo(
                rowDestination,
                indirectDestination,
                indirectDataStart,
                out rowBytesWritten,
                out indirectBytesWritten);

            rowBytesWritten += 1;
        }
    }
}
