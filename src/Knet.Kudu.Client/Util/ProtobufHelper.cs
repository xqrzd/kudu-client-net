using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Rpc;
using ProtoBuf;

namespace Knet.Kudu.Client.Util
{
    public static class ProtobufHelper
    {
        public static RowOperationsPB EncodeRowOperations(params PartialRowOperation[] rows)
        {
            return EncodeRowOperations(rows.ToList());
        }

        public static RowOperationsPB EncodeRowOperations(List<PartialRowOperation> rows)
        {
            OperationsEncoder.ComputeSize(
                rows,
                out int rowSize,
                out int indirectSize);

            var rowData = new byte[rowSize];
            var indirectData = new byte[indirectSize];

            OperationsEncoder.Encode(rows, rowData, indirectData);

            return new RowOperationsPB
            {
                Rows = rowData,
                IndirectData = indirectData
            };
        }

        public static ErrorStatusPB GetErrorStatus(ReadOnlySequence<byte> buffer)
        {
            return Serializer.Deserialize<ErrorStatusPB>(buffer);
        }

        public static bool TryParseResponseHeader(
            ref SequenceReader<byte> reader, long length, out ResponseHeader header)
        {
            if (reader.Remaining < length)
            {
                header = null;
                return false;
            }

            var slice = reader.Sequence.Slice(reader.Position, length);
            header = Serializer.Deserialize<ResponseHeader>(slice);

            reader.Advance(length);

            return true;
        }
    }
}
