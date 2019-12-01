using System.Buffers;
using System.IO;
using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Requests
{
    public class CreateTableRequest : KuduMasterRpc<CreateTableResponsePB>
    {
        private readonly CreateTableRequestPB _request;

        public override string MethodName => "CreateTable";

        public CreateTableRequest(CreateTableRequestPB request)
        {
            _request = request;
        }

        public override void Serialize(Stream stream)
        {
            Serialize(stream, _request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = Deserialize(buffer);
            Error = Output.Error;
        }
    }
}
