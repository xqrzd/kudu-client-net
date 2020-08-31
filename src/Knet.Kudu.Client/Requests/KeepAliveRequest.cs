using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Tablet;
using ProtoBuf;

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
            _request = new ScannerKeepAliveRequestPB { ScannerId = scannerId };

            ReplicaSelection = replicaSelection;
            TableId = tableId;
            Tablet = tablet;
            PartitionKey = partitionKey;
        }

        public override string MethodName => "ScannerKeepAlive";

        public override ReplicaSelection ReplicaSelection { get; }

        public override void Serialize(Stream stream)
        {
            Serializer.SerializeWithLengthPrefix(stream, _request, PrefixStyle.Base128);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var responsePb = Serializer.Deserialize<ScannerKeepAliveResponsePB>(buffer);

            if (responsePb.Error == null)
                Output = responsePb;
            else
                Error = responsePb.Error;
        }
    }
}
