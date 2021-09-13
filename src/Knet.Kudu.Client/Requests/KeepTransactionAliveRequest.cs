using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal class KeepTransactionAliveRequest : KuduTxnRpc<KeepTransactionAliveResponsePB>
    {
        private readonly KeepTransactionAliveRequestPB _request;

        public KeepTransactionAliveRequest(KeepTransactionAliveRequestPB request)
        {
            _request = request;
        }

        public override string MethodName => "KeepTransactionAlive";

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseResponse(KuduMessage message)
        {
            Output = KeepTransactionAliveResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
