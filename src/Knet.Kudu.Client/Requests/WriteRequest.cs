using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests;

internal sealed class WriteRequest : KuduTabletRpc<WriteResponsePB>
{
    private readonly WriteRequestPB _request;

    public WriteRequest(
        WriteRequestPB request,
        string tableId,
        byte[] partitionKey,
        ExternalConsistencyMode externalConsistencyMode)
    {
        _request = request;
        MethodName = "Write";
        TableId = tableId;
        PartitionKey = partitionKey;
        NeedsAuthzToken = true;
        IsRequestTracked = true;
        ExternalConsistencyMode = externalConsistencyMode;
    }

    public override int CalculateSize()
    {
        if (AuthzToken != null)
            _request.AuthzToken = AuthzToken;

        if (PropagatedTimestamp != KuduClient.NoTimestamp)
            _request.PropagatedTimestamp = (ulong)PropagatedTimestamp;

        _request.TabletId = ByteString.CopyFromUtf8(Tablet!.TabletId);

        return _request.CalculateSize();
    }

    public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

    public override void ParseResponse(KuduMessage message)
    {
        Output = WriteResponsePB.Parser.ParseFrom(message.MessageProtobuf);
        Error = Output.Error;
    }
}
