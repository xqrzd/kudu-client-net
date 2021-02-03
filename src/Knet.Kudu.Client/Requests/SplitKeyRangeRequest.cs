using System.Buffers;
using System.Collections.Generic;
using System.IO;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Util;
using ProtoBuf;

namespace Knet.Kudu.Client.Requests
{
    public class SplitKeyRangeRequest : KuduTabletRpc<List<KeyRangePB>>
    {
        private readonly byte[] _startPrimaryKey;
        private readonly byte[] _endPrimaryKey;
        private readonly long _splitSizeBytes;

        public SplitKeyRangeRequest(
            string tableId,
            byte[] startPrimaryKey,
            byte[] endPrimaryKey,
            byte[] partitionKey,
            long splitSizeBytes)
        {
            _startPrimaryKey = startPrimaryKey;
            _endPrimaryKey = endPrimaryKey;
            _splitSizeBytes = splitSizeBytes;

            TableId = tableId;
            PartitionKey = partitionKey;
            NeedsAuthzToken = true;
        }

        public override string MethodName => "SplitKeyRange";

        public override void Serialize(Stream stream)
        {
            var request = new SplitKeyRangeRequestPB
            {
                TabletId = Tablet.TabletId.ToUtf8ByteArray(),
                TargetChunkSizeBytes = (ulong)_splitSizeBytes,
                AuthzToken = AuthzToken
            };

            if (_startPrimaryKey != null && _startPrimaryKey.Length > 0)
                request.StartPrimaryKey = _startPrimaryKey;

            if (_endPrimaryKey != null && _endPrimaryKey.Length > 0)
                request.StopPrimaryKey = _endPrimaryKey;

            Serialize(stream, request);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var result = Serializer.Deserialize<SplitKeyRangeResponsePB>(buffer);

            if (result.Error == null)
                Output = result.Ranges;
            else
                Error = result.Error;
        }
    }
}
