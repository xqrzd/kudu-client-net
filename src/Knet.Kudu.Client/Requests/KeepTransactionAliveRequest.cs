using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal sealed class KeepTransactionAliveRequest : KuduTxnRpc<KeepTransactionAliveResponsePB>
    {
        private readonly KeepTransactionAliveRequestPB _request;

        public KeepTransactionAliveRequest(KeepTransactionAliveRequestPB request)
        {
            MethodName = "KeepTransactionAlive";
            _request = request;
        }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = KeepTransactionAliveResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
