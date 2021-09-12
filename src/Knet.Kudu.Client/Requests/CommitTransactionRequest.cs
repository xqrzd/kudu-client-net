using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal class CommitTransactionRequest : KuduTxnRpc<CommitTransactionResponsePB>
    {
        private readonly CommitTransactionRequestPB _request;

        public CommitTransactionRequest(CommitTransactionRequestPB request)
        {
            _request = request;
        }

        public override string MethodName => "CommitTransaction";

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = CommitTransactionResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
