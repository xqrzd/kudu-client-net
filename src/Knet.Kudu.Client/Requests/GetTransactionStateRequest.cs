using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal sealed class GetTransactionStateRequest : KuduTxnRpc<GetTransactionStateResponsePB>
    {
        private readonly GetTransactionStateRequestPB _request;

        public GetTransactionStateRequest(GetTransactionStateRequestPB request)
        {
            MethodName = "GetTransactionState";
            _request = request;
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = GetTransactionStateResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
