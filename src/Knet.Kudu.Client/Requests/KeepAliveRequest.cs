using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Requests
{
    public class KeepAliveRequest : KuduTabletRpc<ScannerKeepAliveResponsePB>
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

            ReplicaSelection = replicaSelection;
            TableId = tableId;
            Tablet = tablet;
            PartitionKey = partitionKey;
        }

        public override string MethodName => "ScannerKeepAlive";

        public override ReplicaSelection ReplicaSelection { get; }

        public override int CalculateSize() => _request.CalculateSize();

        public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            Output = ScannerKeepAliveResponsePB.Parser.ParseFrom(buffer);
            Error = Output.Error;
        }
    }
}
