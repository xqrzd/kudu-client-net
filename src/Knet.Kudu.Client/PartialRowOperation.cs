using System;

namespace Knet.Kudu.Client
{
    public class PartialRowOperation : PartialRow
    {
        private readonly RowOperation _operation;

        public PartialRowOperation(KuduSchema schema, RowOperation operation)
            : base(schema)
        {
            _operation = operation;
        }

        internal PartialRowOperation(PartialRowOperation row, RowOperation operation)
            : base(row)
        {
            _operation = operation;
        }

        public void WriteToWithOperation(
            Span<byte> rowDestination,
            Span<byte> indirectDestination,
            int indirectDataStart,
            out int rowBytesWritten,
            out int indirectBytesWritten)
        {
            rowDestination[0] = (byte)_operation;
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
