using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Kudu.Client.Protocol.Rpc;

namespace Kudu.Client.Negotiate
{
    public class KuduTlsStream : Stream
    {
        private Negotiator _negotiator;
        private NetworkStream _networkStream;
        private NegotiatePB _result;
        private int _read;
        private bool _authenticated;

        public KuduTlsStream(Negotiator negotiator)
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

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_authenticated)
                return _networkStream.WriteAsync(buffer, cancellationToken);
            else
                return SendHandshakeAsync(buffer);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_authenticated)
                return _networkStream.ReadAsync(buffer, cancellationToken);
            else
                return ReadHandshakeAsync(buffer);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // TODO
            return WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            // TODO
            return ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            // TODO
            WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count)).GetAwaiter().GetResult();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            // TODO
            return ReadAsync(new Memory<byte>(buffer, offset, count)).GetAwaiter().GetResult();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            if (_authenticated)
                return _networkStream.FlushAsync(cancellationToken);
            else
                return Task.CompletedTask;
        }

        public override void Flush()
        {
            _networkStream?.Flush();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public void Complete(NetworkStream stream)
        {
            _authenticated = true;
            _networkStream = stream;

            _negotiator = null;
            _result = null;
        }

        private async ValueTask SendHandshakeAsync(ReadOnlyMemory<byte> buffer)
        {
            _read = 0;
            _result = await _negotiator.SendTlsHandshakeAsync(buffer.ToArray()).ConfigureAwait(false);
        }

        private ValueTask<int> ReadHandshakeAsync(Memory<byte> buffer)
        {
            var length = buffer.Length;
            var tlsHandshake = _result.TlsHandshake.AsSpan(_read, length);
            tlsHandshake.CopyTo(buffer.Span);
            _read += length;
            return new ValueTask<int>(length);
        }
    }
}
