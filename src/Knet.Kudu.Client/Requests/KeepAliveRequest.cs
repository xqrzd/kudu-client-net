using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Requests;

internal sealed class KeepAliveRequest : KuduTabletRpc<ScannerKeepAliveResponsePB>
{
    private readonly ScannerKeepAliveRequestPB _request;

    public KeepAliveRequest(
        byte[] scannerId,
        ReplicaSelection replicaSelection,
        string tableId,
        RemoteTablet tablet,
        byte[] partitionKey)
    {
        _request = new ScannerKeepAliveRequestPB
        {
            ScannerId = UnsafeByteOperations.UnsafeWrap(scannerId),
        };

        MethodName = "ScannerKeepAlive";
        ReplicaSelection = replicaSelection;
        TableId = tableId;
        Tablet = tablet;
        PartitionKey = partitionKey;
    }

    public override int CalculateSize() => _request.CalculateSize();

    public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

    public override void ParseResponse(KuduMessage message)
    {
        Output = ScannerKeepAliveResponsePB.Parser.ParseFrom(message.MessageProtobuf);
        Error = Output.Error;
    }
}
