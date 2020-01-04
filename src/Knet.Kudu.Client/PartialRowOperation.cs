using System;

namespace Knet.Kudu.Client
{
    public class PartialRowOperation : PartialRow
    {
        private readonly RowOperation _operation;

        public PartialRowOperation(Schema schema, RowOperation operation)
            : base(schema)
        {
            _operation = operation;
        }

        internal int RowSizeWithOperation => RowSize + 1;

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
