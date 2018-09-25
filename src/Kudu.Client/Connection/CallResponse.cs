using System;
using System.Buffers;
using System.IO;
using System.Threading;
using Kudu.Client.Protocol.Rpc;
using ProtoBuf;

namespace Kudu.Client.Connection
{
    public class CallResponse : IMemoryOwner<byte>
    {
        private readonly int _offset;
        private readonly int _length;
        private byte[] _oversized;

        public ResponseHeader Header { get; }

        internal CallResponse(ResponseHeader header, byte[] oversized, int offset, int length)
        {
            Header = header;
            _offset = offset;
            _length = length;
            _oversized = oversized;
        }

        public Memory<byte> Memory => new Memory<byte>(GetArray(), _offset, _length);

        private byte[] GetArray() =>
            Interlocked.CompareExchange(ref _oversized, null, null)
            ?? throw new ObjectDisposedException(ToString());

        public T ParseResponse<T>()
        {
            // TODO: Use span/memory when protobuf-3 is ready.
            using (var ms = new MemoryStream(_oversized, _offset, _length))
            {
                var output = Serializer.DeserializeWithLengthPrefix<T>(ms, PrefixStyle.Base128);
                return output;
            }
        }

        // TODO: Expose sidecars here.

        public void Dispose()
        {
            var arr = Interlocked.Exchange(ref _oversized, null);
            if (arr != null)
                ArrayPool<byte>.Shared.Return(arr);
        }

        internal static CallResponse FromMemory(byte[] buffer, int length)
        {
            // TODO: Use span/memory when protobuf-3 is ready.
            using (var ms = new MemoryStream(buffer))
            {
                var responseHeader = Serializer.DeserializeWithLengthPrefix<ResponseHeader>(ms, PrefixStyle.Base128);
                return new CallResponse(responseHeader, buffer, (int)ms.Position, length - (int)ms.Position);
            }
        }
    }
}
