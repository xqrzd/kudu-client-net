using System;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client.Protocol
{
    public sealed class KuduMessage
    {
        private ArrayPoolBuffer<byte> _messageBuffer;
        private int _messageProtobufLength;
        private SidecarOffset[] _sidecarOffsets;

        internal void Init(
            ArrayPoolBuffer<byte> messageBuffer,
            int messageProtobufLength,
            SidecarOffset[] sidecarOffsets)
        {
            _messageBuffer = messageBuffer;
            _messageProtobufLength = messageProtobufLength;
            _sidecarOffsets = sidecarOffsets;
        }

        public byte[] Buffer => _messageBuffer.Buffer;

        public ReadOnlySpan<byte> MessageProtobuf =>
            Buffer.AsSpan(0, _messageProtobufLength);

        public SidecarOffset GetSidecarOffset(int sidecar) => _sidecarOffsets[sidecar];

        internal ArrayPoolBuffer<byte> TakeMemory()
        {
            var messageBuffer = _messageBuffer;
            _messageBuffer = null;
            return messageBuffer;
        }

        internal void Reset()
        {
            var buffer = _messageBuffer;
            if (buffer is not null)
            {
                _messageBuffer = null;
                buffer.Dispose();
            }

            _messageProtobufLength = 0;
            _sidecarOffsets = null;
        }
    }
}
