using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kudu.Client.Protocol.Rpc;

namespace Kudu.Client.Negotiate
{
    public sealed class KuduTlsAuthenticationStream : Stream
    {
        private readonly Negotiator _negotiator;

        private NegotiatePB _result;
        private int _readPosition;

        public KuduTlsAuthenticationStream(Negotiator negotiator)
        {
            _negotiator = negotiator;
        }

        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;

        public override long Length => throw new NotImplementedException();

        public override long Position
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            SendHandshakeAsync(new ReadOnlyMemory<byte>(buffer, offset, count)).GetAwaiter().GetResult();
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return SendHandshakeAsync(new ReadOnlyMemory<byte>(buffer, offset, count)).AsTask();
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return SendHandshakeAsync(buffer);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadHandshake(new Memory<byte>(buffer, offset, count));
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return Task.FromResult(ReadHandshake(new Memory<byte>(buffer, offset, count)));
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            return new ValueTask<int>(ReadHandshake(buffer));
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        private async ValueTask SendHandshakeAsync(ReadOnlyMemory<byte> buffer)
        {
            // TODO: Wire up a cancellation token.
            _readPosition = 0;
            _result = await _negotiator.SendTlsHandshakeAsync(buffer.ToArray()).ConfigureAwait(false);
        }

        private int ReadHandshake(Memory<byte> buffer)
        {
            var length = buffer.Length;
            _result.TlsHandshake.AsSpan(_readPosition, length).CopyTo(buffer.Span);
            _readPosition += length;
            return length;
        }
    }
}
