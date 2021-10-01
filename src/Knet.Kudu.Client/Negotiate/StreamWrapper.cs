using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.Negotiate;

/// <summary>
/// Wraps a <see cref="Stream"/> and allows the inner stream to be replaced.
/// </summary>
public sealed class StreamWrapper : Stream
{
    private Stream _innerStream;

    public StreamWrapper(Stream innerStream)
    {
        _innerStream = innerStream;
    }

    public override bool CanWrite => _innerStream.CanWrite;

    public override bool CanRead => _innerStream.CanRead;

    public override bool CanSeek => _innerStream.CanSeek;

    public override long Length => _innerStream.Length;

    public override long Position
    {
        get => _innerStream.Position;
        set => _innerStream.Position = value;
    }

    public void ReplaceStream(Stream newInnerStream)
    {
        _innerStream = newInnerStream;
    }

    public override void Close()
    {
        _innerStream.Close();
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _innerStream.Write(buffer, offset, count);
    }

    public override void WriteByte(byte value)
    {
        _innerStream.WriteByte(value);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return _innerStream.WriteAsync(buffer, offset, count, cancellationToken);
    }

#if !NETSTANDARD2_0
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        _innerStream.Write(buffer);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return _innerStream.WriteAsync(buffer, cancellationToken);
    }
#endif

    public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state)
    {
        return _innerStream.BeginWrite(buffer, offset, count, callback!, state);
    }

    public override void EndWrite(IAsyncResult asyncResult)
    {
        _innerStream.EndWrite(asyncResult);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return _innerStream.Read(buffer, offset, count);
    }

    public override int ReadByte()
    {
        return _innerStream.ReadByte();
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return _innerStream.ReadAsync(buffer, offset, count, cancellationToken);
    }

#if !NETSTANDARD2_0
    public override int Read(Span<byte> buffer)
    {
        return _innerStream.Read(buffer);
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return _innerStream.ReadAsync(buffer, cancellationToken);
    }
#endif

    public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback? callback, object? state)
    {
        return _innerStream.BeginRead(buffer, offset, count, callback!, state);
    }

    public override int EndRead(IAsyncResult asyncResult)
    {
        return _innerStream.EndRead(asyncResult);
    }

    public override void Flush()
    {
        _innerStream.Flush();
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        return _innerStream.FlushAsync(cancellationToken);
    }

    public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
    {
        return _innerStream.CopyToAsync(destination, bufferSize, cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        return _innerStream.Seek(offset, origin);
    }

    public override void SetLength(long value)
    {
        _innerStream.SetLength(value);
    }
}
