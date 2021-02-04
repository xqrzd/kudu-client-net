using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;
using ProtoBuf;

namespace Knet.Kudu.Client.Requests
{
    public class IsAlterTableDoneRequest : KuduMasterRpc<IsAlterTableDoneResponsePB>
    {
        private readonly IsAlterTableDoneRequestPB _request;

        public override string MethodName => "IsAlterTableDone";

        public IsAlterTableDoneRequest(TableIdentifierPB tableId)
        {
            _request = new IsAlterTableDoneRequestPB { Table = tableId };
        }

        public override void Serialize(Stream stream)
        {
            Serialize(stream, _request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var responsePb = Serializer.Deserialize<IsAlterTableDoneResponsePB>(buffer);

            if (responsePb.Error == null)
                Output = responsePb;
            else
                Error = responsePb.Error;
        }
    }
}
