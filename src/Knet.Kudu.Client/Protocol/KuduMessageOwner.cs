using System;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client.Protocol
{
    public sealed class KuduMessageOwner : IDisposable
    {
        private readonly ArrayPoolBuffer<byte> _messageBuffer;
        private readonly int _messageProtobufLength;
        private readonly RepeatedField<uint> _sidecarOffsets;

        internal KuduMessageOwner(
            ArrayPoolBuffer<byte> messageBuffer,
            int messageProtobufLength,
            RepeatedField<uint> sidecarOffsets)
        {
            _messageBuffer = messageBuffer;
            _messageProtobufLength = messageProtobufLength;
            _sidecarOffsets = sidecarOffsets;
        }

        public byte[] Buffer => _messageBuffer.Buffer;

        public ReadOnlySpan<byte> MessageProtobuf =>
            Buffer.AsSpan(0, _messageProtobufLength);

        public int GetSidecarOffset(int sidecar) => (int)_sidecarOffsets[sidecar];

        public void Dispose()
        {
            _messageBuffer.Dispose();
        }
    }
}
