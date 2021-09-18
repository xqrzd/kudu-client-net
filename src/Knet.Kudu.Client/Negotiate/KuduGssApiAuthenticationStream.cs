// Copyright (c) 2002-2019, Npgsql
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL NPGSQL BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
// SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
// ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
// Npgsql HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// NPGSQL SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
// TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND Npgsql
// HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS,
// OR MODIFICATIONS.

using System;
using System.IO;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf.Rpc;
using static Knet.Kudu.Client.Protobuf.Rpc.NegotiatePB.Types;

namespace Knet.Kudu.Client.Negotiate;

public sealed class KuduGssApiAuthenticationStream : Stream
{
    private const int HandshakeDoneId = 20;
    private const int HandshakeErrId = 21;
    private const int HandshakeId = 22;
    private const int DefaultMajorV = 1;
    private const int DefaultMinorV = 0;

    private readonly Negotiator _negotiator;
    private NegotiateStream _negotiateStream;

    private bool _negotiatePhase;
    private NegotiatePB _negotiatePB;
    private int _leftToWrite;

    private ReadOnlyMemory<byte> _writeQueue;
    private ReadOnlyMemory<byte> _readQueue;

    public KuduGssApiAuthenticationStream(Negotiator negotiator)
    {
        _negotiator = negotiator;
        _negotiatePhase = true;
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
        WriteAsyncInternal(new ReadOnlyMemory<byte>(buffer, offset, count), default)
            .AsTask().GetAwaiter().GetResult();
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return WriteAsyncInternal(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).AsTask();
    }

#if !NETSTANDARD2_0
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return WriteAsyncInternal(buffer, cancellationToken);
    }
#endif

    public override int Read(byte[] buffer, int offset, int count)
    {
        return ReadInternal(new Memory<byte>(buffer, offset, count));
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return Task.FromResult(ReadInternal(new Memory<byte>(buffer, offset, count)));
    }

#if !NETSTANDARD2_0
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return new ValueTask<int>(ReadInternal(buffer));
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

    public void CompleteNegotiate(NegotiateStream negotiateStream)
    {
        _negotiatePhase = false;
        _negotiateStream = negotiateStream;
    }

    public Memory<byte> EncryptBuffer(ReadOnlyMemory<byte> buffer)
    {
        _negotiateStream.Write(buffer.Span);

        // Capture the value NegotiateStream just wrote and return it.
        var value = _writeQueue;
        _writeQueue = default;
        return value.ToArray();
    }

    public Memory<byte> DecryptBuffer(ReadOnlyMemory<byte> buffer)
    {
        // Setup NegotiateStream's inner stream (this)
        // to read the given buffer;
        _readQueue = buffer;

        Memory<byte> decryptedBuffer = new byte[buffer.Length * 2];
        var read = _negotiateStream.Read(decryptedBuffer.Span);

        return decryptedBuffer.Slice(0, read);
    }

    private async ValueTask WriteAsyncInternal(
        ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_negotiatePhase)
        {
            if (_leftToWrite == 0)
            {
                // We're writing the frame header, which contains the payload size.
                _leftToWrite = ReadFrameHeaderLength(buffer.Span);

                // In case of payload data in the same buffer just after the frame header.
                if (buffer.Length == 5)
                    return;

                buffer = buffer.Slice(5);
            }

            if (buffer.Length > _leftToWrite)
                throw new Exception($"NegotiateStream trying to write {buffer.Length} bytes but according to frame header we only have {_leftToWrite} left!");

            // TODO: Handle the case where we don't get the entire buffer.
            // TODO: Move this to a separate method.
            _negotiatePB = await _negotiator
                .SendGssApiTokenAsync(NegotiateStep.SaslInitiate, buffer, cancellationToken)
                .ConfigureAwait(false);

            _readQueue = CreateReadHeader(_negotiatePB.Token.Memory);
            _leftToWrite -= buffer.Length;
        }
        else
        {
            _writeQueue = buffer;
        }
    }

    private int ReadInternal(Memory<byte> buffer)
    {
        var length = Math.Min(buffer.Length, _readQueue.Length);

        _readQueue.Slice(0, length).CopyTo(buffer);
        _readQueue = _readQueue.Slice(length);

        return length;
    }

    private static int ReadFrameHeaderLength(ReadOnlySpan<byte> buffer)
    {
        if (buffer[0] != 22)
            throw new Exception($"Expected HandshakeId (22), instead received {buffer[0]}");

        if (buffer[1] != DefaultMajorV)
            throw new NotSupportedException($"Received frame header major v {buffer[1]} (different from {DefaultMajorV})");
        if (buffer[2] != DefaultMinorV)
            throw new NotSupportedException($"Received frame header minor v {buffer[2]} (different from {DefaultMinorV})");

        var length = (buffer[3] << 8) | buffer[4];
        return length;
    }

    private static ReadOnlyMemory<byte> CreateReadHeader(ReadOnlyMemory<byte> token)
    {
        // TODO: Verify token success

        var length = token.Length;
        var buffer = new Memory<byte>(new byte[length + 5]);
        var span = buffer.Span;

        span[0] = HandshakeDoneId;
        span[1] = DefaultMajorV;
        span[2] = DefaultMinorV;
        span[3] = (byte)((length >> 8) & 0xFF);
        span[4] = (byte)(length & 0xFF);

        token.CopyTo(buffer.Slice(5));

        return buffer;
    }
}
