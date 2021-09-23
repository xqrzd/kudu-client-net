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
using System.Buffers;
using System.IO;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client.Negotiate;

public sealed class KuduGssApiAuthenticationStream : Stream
{
    private const int HandshakeDoneId = 20;
    private const int HandshakeId = 22;
    private const int DefaultMajorV = 1;
    private const int DefaultMinorV = 0;

    private readonly Negotiator _negotiator;

    private NegotiateStream _negotiateStream;
    private Memory<byte> _queue;
    private int _leftToWrite;

    public KuduGssApiAuthenticationStream(Negotiator negotiator)
    {
        _negotiator = negotiator;
    }

    private bool IsAuthenticationPhase => _negotiateStream is null;

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
        var memory = new ReadOnlyMemory<byte>(buffer, offset, count);

        if (IsAuthenticationPhase)
        {
            SendSaslInitiateAsync(memory).GetAwaiter().GetResult();
        }
        else
        {
            _queue = memory.ToArray();
        }
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var memory = new ReadOnlyMemory<byte>(buffer, offset, count);

        if (IsAuthenticationPhase)
        {
            return SendSaslInitiateAsync(memory, cancellationToken);
        }
        else
        {
            _queue = memory.ToArray();
            return Task.CompletedTask;
        }
    }

#if !NETSTANDARD2_0
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (IsAuthenticationPhase)
        {
            var task = SendSaslInitiateAsync(buffer, cancellationToken);
            return new ValueTask(task);
        }
        else
        {
            _queue = buffer.ToArray();
            return new ValueTask();
        }
    }
#endif

    public override int Read(byte[] buffer, int offset, int count)
    {
        var memory = buffer.AsMemory(offset, count);

        if (IsAuthenticationPhase)
        {
            return ReceiveSaslInitiateAsync(memory).GetAwaiter().GetResult();
        }
        else
        {
            return ReadInternalQueue(memory);
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var memory = buffer.AsMemory(offset, count);

        if (IsAuthenticationPhase)
        {
            return ReceiveSaslInitiateAsync(memory, cancellationToken);
        }
        else
        {
            return Task.FromResult(ReadInternalQueue(memory));
        }
    }

#if !NETSTANDARD2_0
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (IsAuthenticationPhase)
        {
            var task = ReceiveSaslInitiateAsync(buffer, cancellationToken);
            return new ValueTask<int>(task);
        }
        else
        {
            return new ValueTask<int>(ReadInternalQueue(buffer));
        }
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
        _negotiateStream = negotiateStream;
    }

    public Memory<byte> EncryptBuffer(ReadOnlySpan<byte> buffer)
    {
        _negotiateStream.Write(buffer);

        // Capture the value NegotiateStream just wrote and return it.
        var value = _queue;
        _queue = default;
        return value;
    }

    public ReadOnlyMemory<byte> DecryptBuffer(ReadOnlyMemory<byte> buffer)
    {
        // Setup NegotiateStream's inner stream (this) to read the given buffer.
        _queue = MemoryMarshal.AsMemory(buffer);

        var writer = new ArrayBufferWriter<byte>(32);

        while (true)
        {
            var span = writer.GetSpan();
            var read = _negotiateStream.Read(span);

            if (read > 0)
            {
                writer.Advance(read);
            }
            else
            {
                break;
            }
        }

        return writer.WrittenMemory;
    }

    private async Task SendSaslInitiateAsync(
        ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (_leftToWrite == 0)
        {
            // We're writing the frame header, which contains the payload size.
            _leftToWrite = ReadFrameHeaderLength(buffer.Span);
            buffer = buffer.Slice(5);
        }

        if (buffer.Length == 0)
        {
            return;
        }

        if (buffer.Length > _leftToWrite)
            throw new Exception($"NegotiateStream trying to write {buffer.Length} bytes but according to frame header we only have {_leftToWrite} left!");

        await _negotiator.SendSaslInitiateAsync(buffer, cancellationToken).ConfigureAwait(false);

        _leftToWrite -= buffer.Length;
    }

    private async Task<int> ReceiveSaslInitiateAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0)
        {
            return 0;
        }

        var queue = _queue;
        if (queue.Length == 0)
        {
            var result = await _negotiator.ReceiveResponseAsync(cancellationToken)
                .ConfigureAwait(false);

            queue = AddFrameHeader(result.Token.Span);
        }

        int length = Math.Min(buffer.Length, queue.Length);
        queue.Slice(0, length).CopyTo(buffer);
        _queue = queue.Slice(length);

        return length;
    }

    private int ReadInternalQueue(Memory<byte> buffer)
    {
        var queue = _queue;
        int length = Math.Min(buffer.Length, queue.Length);

        queue.Slice(0, length).CopyTo(buffer);
        _queue = queue.Slice(length);

        return length;
    }

    private static int ReadFrameHeaderLength(ReadOnlySpan<byte> buffer)
    {
        if (buffer[0] != HandshakeId)
            throw new ArgumentException($"Expected HandshakeId ({HandshakeId}), instead received {buffer[0]}");

        if (buffer[1] != DefaultMajorV)
            throw new NotSupportedException($"Received frame header major v {buffer[1]} (different from {DefaultMajorV})");
        if (buffer[2] != DefaultMinorV)
            throw new NotSupportedException($"Received frame header minor v {buffer[2]} (different from {DefaultMinorV})");

        var length = (buffer[3] << 8) | buffer[4];
        return length;
    }

    private static byte[] AddFrameHeader(ReadOnlySpan<byte> token)
    {
        var length = token.Length;
        var buffer = new byte[length + 5];

        buffer[0] = HandshakeDoneId;
        buffer[1] = DefaultMajorV;
        buffer[2] = DefaultMinorV;
        buffer[3] = (byte)((length >> 8) & 0xFF);
        buffer[4] = (byte)(length & 0xFF);

        token.CopyTo(buffer.AsSpan(5));

        return buffer;
    }
}
