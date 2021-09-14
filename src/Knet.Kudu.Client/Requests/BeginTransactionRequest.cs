using System.Buffers;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Requests
{
    internal sealed class BeginTransactionRequest : KuduTxnRpc<BeginTransactionResponsePB>
    {
        private static readonly byte[] _requestBytes = ProtobufHelper.ToByteArray(
            new BeginTransactionRequestPB());

        public BeginTransactionRequest()
        {
            MethodName = "BeginTransaction";
        }

        public override int CalculateSize() => _requestBytes.Length;

        public override void WriteTo(IBufferWriter<byte> output) => output.Write(_requestBytes);

        public override void ParseResponse(KuduMessage message)
        {
            Output = BeginTransactionResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            Error = Output.Error;
        }
    }
}
