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

        public void WriteToWithOperation(Span<byte> buffer, Span<byte> indirectData)
        {
            buffer[0] = (byte)_operation;
            buffer = buffer.Slice(1);

            WriteTo(buffer, indirectData);
        }
    }
}
