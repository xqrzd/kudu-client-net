using System;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client.Protocol
{
    public sealed class KuduMessage
    {
        private ArrayPoolBuffer<byte> _messageBuffer;
        private int _messageProtobufLength;
        private RepeatedField<uint> _sidecarOffsets;

        internal void Init(
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

        public KuduMessageOwner TakeOwnership()
        {
            var kuduMessageOwner = new KuduMessageOwner(
                _messageBuffer,
                _messageProtobufLength,
                _sidecarOffsets);

            _messageBuffer = null;

            return kuduMessageOwner;
        }

        internal void Reset()
        {
            _messageBuffer?.Dispose();
            _messageBuffer = null;
            _messageProtobufLength = 0;
            _sidecarOffsets = null;
        }
    }
}
