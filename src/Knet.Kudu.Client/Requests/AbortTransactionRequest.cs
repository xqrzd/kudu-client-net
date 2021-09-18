using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests;

internal sealed class AbortTransactionRequest : KuduTxnRpc<AbortTransactionResponsePB>
{
    private readonly AbortTransactionRequestPB _request;

    public AbortTransactionRequest(AbortTransactionRequestPB request)
    {
        MethodName = "AbortTransaction";
        _request = request;
    }

    public override int CalculateSize() => _request.CalculateSize();

    public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

    public override void ParseResponse(KuduMessage message)
    {
        Output = AbortTransactionResponsePB.Parser.ParseFrom(message.MessageProtobuf);
        Error = Output.Error;
    }
}
