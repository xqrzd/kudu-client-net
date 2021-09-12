using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests
{
    internal class BeginTransactionRequest : KuduTxnRpc<BeginTransactionResponsePB>
    {
        private static readonly byte[] _requestBytes = new BeginTransactionRequestPB().ToByteArray();

        public override string MethodName => "BeginTransaction";

        public override int CalculateSize() => _requestBytes.Length;

        public override void WriteTo(IBufferWriter<byte> output) => output.Write(_requestBytes);

        public override void ParseResponse(KuduMessage message)
        {
            Output = BeginTransactionResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
