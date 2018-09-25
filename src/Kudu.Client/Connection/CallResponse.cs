using System;
using System.Buffers;
using System.IO;
using Kudu.Client.Protocol.Rpc;
using ProtoBuf;

namespace Kudu.Client.Connection
{
    // TODO: Rethink this class.
    public class CallResponse : IDisposable
    {
        private readonly IMemoryOwner<byte> _memoryOwner;

        public Memory<byte> Memory { get; }

        public ResponseHeader Header { get; }

        public CallResponse(
            ResponseHeader header,
            IMemoryOwner<byte> memoryOwner,
            Memory<byte> memory)
        {
            Header = header;
            _memoryOwner = memoryOwner;
            Memory = memory;
        }

        // TODO: Expose sidecars here.

        public void Dispose()
        {
            _memoryOwner?.Dispose();
        }

        public T ParseResponse<T>()
        {
            // TODO: Remove this wasteful copying when protobuf-net 3 is ready.
            using (var ms = new MemoryStream(Memory.ToArray()))
            {
                var output = Serializer.DeserializeWithLengthPrefix<T>(ms, PrefixStyle.Base128);
                return output;
            }
        }

        public static CallResponse FromMemory(IMemoryOwner<byte> memoryOwner, int length)
        {
            var memory = memoryOwner.Memory.Slice(0, length);

            // TODO: Remove this wasteful copying when protobuf-net 3 is ready.
            using (var ms = new MemoryStream(memory.ToArray()))
            {
                var responseHeader = Serializer.DeserializeWithLengthPrefix<ResponseHeader>(ms, PrefixStyle.Base128);
                var body = memory.Slice((int)ms.Position);

                return new CallResponse(responseHeader, memoryOwner, body);
            }
        }
    }
}
