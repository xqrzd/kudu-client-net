using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;

namespace Knet.Kudu.Client.Requests
{
    public class IsCreateTableDoneRequest : KuduMasterRpc<IsCreateTableDoneResponsePB>
    {
        private readonly IsCreateTableDoneRequestPB _request;

        public override string MethodName => "IsCreateTableDone";

        public IsCreateTableDoneRequest(TableIdentifierPB tableId)
        {
            _request = new IsCreateTableDoneRequestPB { Table = tableId };
        }

        public override void Serialize(Stream stream)
        {
            Serialize(stream, _request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var responsePb = Deserialize(buffer);

            if (responsePb.Error == null)
                Output = responsePb;
            else
                Error = responsePb.Error;
        }
    }
}
