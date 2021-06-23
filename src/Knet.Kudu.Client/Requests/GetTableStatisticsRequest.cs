using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;

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

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = GetTableStatisticsResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
