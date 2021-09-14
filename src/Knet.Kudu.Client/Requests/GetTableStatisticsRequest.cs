using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal sealed class GetTableStatisticsRequest : KuduMasterRpc<GetTableStatisticsResponsePB>
    {
        private readonly GetTableStatisticsRequestPB _request;

        public GetTableStatisticsRequest(GetTableStatisticsRequestPB request)
        {
            MethodName = "GetTableStatistics";
            _request = request;
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = GetTableStatisticsResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
