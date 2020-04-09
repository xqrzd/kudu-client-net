using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Master;
using ProtoBuf;

namespace Knet.Kudu.Client.Requests
{
    public class GetTableStatisticsRequest : KuduMasterRpc<GetTableStatisticsResponsePB>
    {
        private readonly GetTableStatisticsRequestPB _request;

        public override string MethodName => "GetTableStatistics";

        public GetTableStatisticsRequest(TableIdentifierPB table)
        {
            _request = new GetTableStatisticsRequestPB { Table = table };
        }

        public override void Serialize(Stream stream)
        {
            Serialize(stream, _request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var responsePb = Serializer.Deserialize<GetTableStatisticsResponsePB>(buffer);

            if (responsePb.Error == null)
                Output = responsePb;
            else
                Error = responsePb.Error;
        }
    }
}
