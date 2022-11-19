using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protobuf.Rpc;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Requests;
using Microsoft.Extensions.Logging;
using static Knet.Kudu.Client.Protobuf.Rpc.ErrorStatusPB.Types;

namespace Knet.Kudu.Client.Connection;

public sealed class KuduConnection : IAsyncDisposable
{
    private const int ReadAtLeastThreshold = 1024 * 32; // 32KB
    private const int MaximumReadSize = 1024 * 512; // 512KB

    private readonly IDuplexPipe _ioPipe;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _singleWriter;
    private readonly Dictionary<int, InflightRpc> _inflightRpcs;
    private readonly CancellationTokenSource _connectionClosedTokenSource;
    private readonly Task _receiveTask;

    private int _nextCallId;

    public KuduConnection(IDuplexPipe ioPipe, ILoggerFactory loggerFactory)
    {
        _ioPipe = ioPipe;
        _logger = loggerFactory.CreateLogger<KuduConnection>();
        _singleWriter = new SemaphoreSlim(1, 1);
        _inflightRpcs = new Dictionary<int, InflightRpc>();
        _connectionClosedTokenSource = new CancellationTokenSource();
        _receiveTask = ReceiveAsync(ioPipe.Input);
    }

    /// <summary>
    /// Triggered when the client connection is closed.
    /// </summary>
    public CancellationToken ConnectionClosed => _connectionClosedTokenSource.Token;

    /// <summary>
    /// Stops accepting RPCs and completes any outstanding RPCs with exceptions.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        _ioPipe.Output.CancelPendingFlush();
        _ioPipe.Input.CancelPendingRead();

        // Wait for the reader loop to finish.
        return new ValueTask(_receiveTask);
    }

    public async Task SendReceiveAsync(
        RequestHeader header,
        KuduRpc rpc,
        CancellationToken cancellationToken)
    {
        var inflightRpc = new InflightRpc(rpc);

        var callId = AddInflightRpc(inflightRpc);
        header.CallId = callId;

        using var _ = cancellationToken.UnsafeRegister(
            static (state, token) => ((InflightRpc)state!).TrySetCanceled(token),
            inflightRpc);

        try
        {
            await SendAsync(header, rpc, cancellationToken).ConfigureAwait(false);
            await inflightRpc.Task.ConfigureAwait(false);
        }
        finally
        {
            RemoveInflightRpc(callId);
        }
    }

    private async ValueTask SendAsync(
        RequestHeader header,
        KuduRpc rpc,
        CancellationToken cancellationToken)
    {
        PipeWriter output = _ioPipe.Output;
        await _singleWriter.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Write(output, header, rpc);

            var result = await output.FlushAsync(cancellationToken).ConfigureAwait(false);

            if (result.IsCanceled)
                await output.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new RecoverableException(
                KuduStatus.NetworkError(ex.Message), ex);
        }
        finally
        {
            _singleWriter.Release();
        }
    }

    private static void Write(PipeWriter output, RequestHeader header, KuduRpc rpc)
    {
        // +------------------------------------------------+
        // | Total message length (4 bytes)                 |
        // +------------------------------------------------+
        // | RPC Header protobuf length (variable encoding) |
        // +------------------------------------------------+
        // | RPC Header protobuf                            |
        // +------------------------------------------------+
        // | Main message length (variable encoding)        |
        // +------------------------------------------------+
        // | Main message protobuf                          |
        // +------------------------------------------------+

        var headerSize = header.CalculateSize();
        var messageSize = rpc.CalculateSize();

        var headerSizeLength = CodedOutputStream.ComputeLengthSize(headerSize);
        var messageSizeLength = CodedOutputStream.ComputeLengthSize(messageSize);

        var totalSize = 4 +
            headerSizeLength + headerSize +
            messageSizeLength + messageSize;

        var totalSizeWithoutMessage = totalSize - messageSize;
        var buffer = output.GetSpan(totalSizeWithoutMessage);

        BinaryPrimitives.WriteInt32BigEndian(buffer, totalSize - 4);
        buffer = buffer.Slice(4);

        ProtobufHelper.WriteRawVarint32(buffer, (uint)headerSize);
        buffer = buffer.Slice(headerSizeLength);

        header.WriteTo(buffer.Slice(0, headerSize));
        buffer = buffer.Slice(headerSize);

        ProtobufHelper.WriteRawVarint32(buffer, (uint)messageSize);

        output.Advance(totalSizeWithoutMessage);
        rpc.WriteTo(output);
    }

    private async Task ReceiveAsync(PipeReader reader)
    {
        Exception? exception = null;
        using var parserContext = new ParserContext();

        try
        {
            while (true)
            {
                var result = await ReadAsync(reader, parserContext).ConfigureAwait(false);

                if (result.IsCanceled)
                {
                    break;
                }

                var buffer = result.Buffer;
                var position = ProcessMessages(buffer, parserContext);

                reader.AdvanceTo(position, buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            exception = ex;
        }
        finally
        {
            await ShutdownAsync(exception).ConfigureAwait(false);
        }
    }

    private SequencePosition ProcessMessages(ReadOnlySequence<byte> buffer, ParserContext parserContext)
    {
        var reader = new SequenceReader<byte>(buffer);

        while (KuduMessageParser.TryParse(ref reader, parserContext))
        {
            HandleRpc(parserContext);
            parserContext.Reset();
        }

        return reader.Position;
    }

    private void HandleRpc(ParserContext parserContext)
    {
        var header = parserContext.Header!;
        var callId = header.CallId;

        if (TryGetInflightRpc(callId, out var inflightRpc))
        {
            var message = parserContext.Message;

            if (header.IsError)
            {
                var error = ErrorStatusPB.Parser.ParseFrom(message.MessageProtobuf);
                var exception = GetException(error);

                inflightRpc.TrySetException(exception);
            }
            else
            {
                try
                {
                    inflightRpc.Rpc.ParseResponse(message);
                    inflightRpc.TrySetResult();
                }
                catch (Exception ex)
                {
                    inflightRpc.TrySetException(ex);
                }
            }
        }
        else
        {
            // We couldn't match the incoming CallId to an inflight RPC, probably
            // because the RPC timed out on our side and was removed from _inflightRpcs.
            _logger.ReceivedUnknownRpc(callId, _ioPipe.ToString()!);
        }
    }

    private Exception GetException(ErrorStatusPB error)
    {
        var code = error.Code;
        KuduException exception;

        if (code == RpcErrorCodePB.ErrorServerTooBusy ||
            code == RpcErrorCodePB.ErrorUnavailable)
        {
            exception = new RecoverableException(
                KuduStatus.ServiceUnavailable(error.Message));
        }
        else if (code == RpcErrorCodePB.FatalInvalidAuthenticationToken)
        {
            exception = new InvalidAuthnTokenException(
                KuduStatus.NotAuthorized(error.Message));
        }
        else if (code == RpcErrorCodePB.ErrorInvalidAuthorizationToken)
        {
            exception = new InvalidAuthzTokenException(
                KuduStatus.NotAuthorized(error.Message));
        }
        else
        {
            var message = $"[peer {_ioPipe}] server sent error {error.Message}";
            exception = new RpcRemoteException(KuduStatus.RemoteError(message), error);
        }

        return exception;
    }

    private int AddInflightRpc(InflightRpc inflightRpc)
    {
        int callId;

        lock (_inflightRpcs)
        {
            callId = _nextCallId++;
            _inflightRpcs.Add(callId, inflightRpc);
        }

        return callId;
    }

    private void RemoveInflightRpc(int callId)
    {
        lock (_inflightRpcs)
        {
            _inflightRpcs.Remove(callId);
        }
    }

    private bool TryGetInflightRpc(int callId, [MaybeNullWhen(false)] out InflightRpc rpc)
    {
        lock (_inflightRpcs)
        {
            return _inflightRpcs.TryGetValue(callId, out rpc);
        }
    }

    /// <summary>
    /// Stops accepting new RPCs and completes any outstanding RPCs with exceptions.
    /// </summary>
    private async Task ShutdownAsync(Exception? exception)
    {
        await _ioPipe.Output.CompleteAsync(exception).ConfigureAwait(false);
        await _ioPipe.Input.CompleteAsync(exception).ConfigureAwait(false);

        if (exception is not null)
            _logger.ConnectionDisconnected(exception, _ioPipe.ToString()!);

        var closedException = new RecoverableException(
            KuduStatus.IllegalState($"Connection {_ioPipe} is disconnected."), exception);

        lock (_inflightRpcs)
        {
            _connectionClosedTokenSource.Cancel();

            foreach (var inflightMessage in _inflightRpcs.Values)
                inflightMessage.TrySetException(closedException);

            _inflightRpcs.Clear();
        }

        // Don't dispose _singleWriter; there may still be pending writes.

        (_ioPipe as IDisposable)?.Dispose();
        _connectionClosedTokenSource.Dispose();
    }

    public override string? ToString() => _ioPipe.ToString();

    private static ValueTask<ReadResult> ReadAsync(PipeReader reader, ParserContext context)
    {
        var readHint = context.ReadHint;

        return readHint > ReadAtLeastThreshold
            ? reader.ReadAtLeastAsync(Math.Min(readHint, MaximumReadSize))
            : reader.ReadAsync();
    }

    private sealed class InflightRpc : TaskCompletionSource
    {
        public KuduRpc Rpc { get; }

        public InflightRpc(KuduRpc rpc)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            Rpc = rpc;
        }
    }
}
