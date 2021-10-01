using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.Negotiate;

public sealed class KuduTlsAuthenticationStream : Stream
{
    private readonly Negotiator _negotiator;

    private ReadOnlyMemory<byte> _tlsHandshake;

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
        return SendHandshakeAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken);
    }

#if !NETSTANDARD2_0
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return new ValueTask(SendHandshakeAsync(buffer, cancellationToken));
    }
#endif

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReceiveHandshakeAsync(new Memory<byte>(buffer, offset, count)).GetAwaiter().GetResult();
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return ReceiveHandshakeAsync(new Memory<byte>(buffer, offset, count), cancellationToken);
    }

#if !NETSTANDARD2_0
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return new ValueTask<int>(ReceiveHandshakeAsync(buffer, cancellationToken));
    }
#endif

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

    private Task SendHandshakeAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        return _negotiator.SendTlsHandshakeAsync(buffer, cancellationToken);
    }

    private async Task<int> ReceiveHandshakeAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0)
        {
            return 0;
        }

        var tlsHandshake = _tlsHandshake;
        if (tlsHandshake.Length == 0)
        {
            var result = await _negotiator.ReceiveResponseAsync(cancellationToken)
                .ConfigureAwait(false);

            tlsHandshake = result.TlsHandshake.Memory;
        }

        int length = Math.Min(buffer.Length, tlsHandshake.Length);
        tlsHandshake.Slice(0, length).CopyTo(buffer);
        _tlsHandshake = tlsHandshake.Slice(length);

        return length;
    }
}
