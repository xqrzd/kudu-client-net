using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests;

internal sealed class SplitKeyRangeRequest : KuduTabletRpc<SplitKeyRangeResponsePB>
{
    private readonly SplitKeyRangeRequestPB _request;

    public SplitKeyRangeRequest(
        string tableId,
        byte[] startPrimaryKey,
        byte[] endPrimaryKey,
        byte[] partitionKey,
        long splitSizeBytes)
    {
        _request = new SplitKeyRangeRequestPB
        {
            TargetChunkSizeBytes = (ulong)splitSizeBytes
        };

        if (startPrimaryKey != null && startPrimaryKey.Length > 0)
            _request.StartPrimaryKey = UnsafeByteOperations.UnsafeWrap(startPrimaryKey);

        if (endPrimaryKey != null && endPrimaryKey.Length > 0)
            _request.StopPrimaryKey = UnsafeByteOperations.UnsafeWrap(endPrimaryKey);

        MethodName = "SplitKeyRange";
        TableId = tableId;
        PartitionKey = partitionKey;
        NeedsAuthzToken = true;
    }

    public override int CalculateSize()
    {
        _request.TabletId = ByteString.CopyFromUtf8(Tablet.TabletId);
        _request.AuthzToken = AuthzToken;

        return _request.CalculateSize();
    }

    public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

    public override void ParseResponse(KuduMessage message)
    {
        Output = SplitKeyRangeResponsePB.Parser.ParseFrom(message.MessageProtobuf);
        Error = Output.Error;
    }
}
