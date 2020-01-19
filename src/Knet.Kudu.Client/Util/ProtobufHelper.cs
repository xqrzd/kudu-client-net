using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Protocol;

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
    }
}
