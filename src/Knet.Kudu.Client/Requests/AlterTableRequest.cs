using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Util;
using ProtoBuf;

namespace Knet.Kudu.Client.Requests
{
    public class AlterTableRequest : KuduMasterRpc<AlterTableResponse>
    {
        private readonly AlterTableRequestPB _request;

        public override string MethodName => "AlterTable";

        public AlterTableRequest(AlterTableRequestPB request)
        {
            _request = request;
        }

        public override void Serialize(Stream stream)
        {
            Serialize(stream, _request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var responsePb = Serializer.Deserialize<AlterTableResponsePB>(buffer);
            
            if (responsePb.Error == null)
            {
                Output = new AlterTableResponse(
                    responsePb.TableId.ToStringUtf8(),
                    responsePb.SchemaVersion);
            }
            else
            {
                Error = responsePb.Error;
            }
        }
    }
}
