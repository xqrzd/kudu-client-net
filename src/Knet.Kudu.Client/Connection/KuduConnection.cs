using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protocol.Rpc;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;
using ProtoBuf;

namespace Knet.Kudu.Client.Connection
{
    public class KuduConnection
    {
        private readonly IDuplexPipe _ioPipe;
        private readonly ILogger _logger;
        private readonly SemaphoreSlim _singleWriter;
        private readonly Dictionary<int, InflightRpc> _inflightRpcs;
        private readonly Task _receiveTask;

        private int _nextCallId;
        private bool _closed;
        private RecoverableException _closedException;
        private DisconnectedCallback _disconnectedCallback;

        public KuduConnection(IDuplexPipe ioPipe, ILoggerFactory loggerFactory)
        {
            _ioPipe = ioPipe;
            _logger = loggerFactory.CreateLogger<KuduConnection>();
            _singleWriter = new SemaphoreSlim(1, 1);
            _inflightRpcs = new Dictionary<int, InflightRpc>();
            _nextCallId = 0;

            _receiveTask = ReceiveAsync();
        }

        private string LogPrefix => $"[peer {_ioPipe}]";

        public void OnDisconnected(Action<Exception, object> callback, object state)
        {
            lock (_inflightRpcs)
            {
                _disconnectedCallback = new DisconnectedCallback(callback, state);

                if (_closed)
                {
                    // Guarantee the disconnected callback is invoked if
                    // the connection is already closed.
                    InvokeDisconnectedCallback();
                }
            }
        }

        public async Task SendReceiveAsync(
            RequestHeader header,
            KuduRpc rpc,
            CancellationToken cancellationToken)
        {
            var message = new InflightRpc(rpc);
            int callId;

            lock (_inflightRpcs)
            {
                callId = _nextCallId++;
                _inflightRpcs.Add(callId, message);
            }

            header.CallId = callId;

            using var registration = cancellationToken.Register(
                s => ((InflightRpc)s).TrySetCanceled(),
                state: message,
                useSynchronizationContext: false);

            try
            {
                await SendAsync(header, rpc, cancellationToken).ConfigureAwait(false);
                await message.Task.ConfigureAwait(false);
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
            // TODO: Use PipeWriter once protobuf-net supports it.
            using (var stream = new RecyclableMemoryStream())
            {
                // Make space to write the length of the entire message.
                stream.GetMemory(4);

                Serializer.SerializeWithLengthPrefix(stream, header, PrefixStyle.Base128);
                rpc.Serialize(stream);

                // Go back and write the length of the entire message, minus the 4
                // bytes we already allocated to store the length.
                BinaryPrimitives.WriteUInt32BigEndian(stream.AsSpan(), (uint)stream.Length - 4);

                await WriteAsync(stream.AsMemory(), cancellationToken).ConfigureAwait(false);
            }
        }

        private async ValueTask WriteAsync(
            ReadOnlyMemory<byte> source,
            CancellationToken cancellationToken)
        {
            PipeWriter output = _ioPipe.Output;
            await _singleWriter.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await output.WriteAsync(source, cancellationToken).ConfigureAwait(false);
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

        private async Task ReceiveAsync()
        {
            var input = _ioPipe.Input;
            var parserContext = new ParserContext();

            try
            {
                while (true)
                {
                    var result = await input.ReadAsync().ConfigureAwait(false);
                    var buffer = result.Buffer;

                    // TODO: Review this. IsCompleted should happen after ParseMessages().
                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    ParseMessages(buffer, parserContext, out var consumed);

                    input.AdvanceTo(consumed, buffer.End);
                }

                await ShutdownAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await ShutdownAsync(ex).ConfigureAwait(false);
            }
        }

        private void ParseMessages(
            ReadOnlySequence<byte> buffer,
            ParserContext parserContext,
            out SequencePosition consumed)
        {
            var reader = new SequenceReader<byte>(buffer);

            do
            {
                if (TryParseMessage(ref reader, parserContext))
                {
                    var rpc = parserContext.InflightRpc;

                    if (parserContext.Exception != null)
                    {
                        CompleteRpc(rpc, parserContext.Exception);
                    }
                    else
                    {
                        CompleteRpc(rpc);
                    }

                    parserContext.Reset();
                }
            } while (parserContext.Step == ParseStep.NotStarted && reader.Remaining >= 4);

            consumed = reader.Position;
        }

        private bool TryParseMessage(
            ref SequenceReader<byte> reader, ParserContext parserContext)
        {
            switch (parserContext.Step)
            {
                case ParseStep.NotStarted:
                    {
                        if (reader.TryReadBigEndian(out parserContext.TotalMessageLength))
                        {
                            goto case ParseStep.ReadHeaderLength;
                        }
                        else
                        {
                            // Not enough data to read message size.
                            break;
                        }
                    }
                case ParseStep.ReadHeaderLength:
                    {
                        if (reader.TryReadVarint(out parserContext.HeaderLength))
                        {
                            goto case ParseStep.ReadHeader;
                        }
                        else
                        {
                            // Not enough data to read header length.
                            parserContext.Step = ParseStep.ReadHeaderLength;
                            break;
                        }
                    }
                case ParseStep.ReadHeader:
                    {
                        if (ProtobufHelper.TryParseResponseHeader(ref reader,
                            parserContext.HeaderLength, out parserContext.Header))
                        {
                            if (!TryGetRpc(parserContext.Header, out parserContext.InflightRpc))
                            {
                                // We don't have this RPC. It probably timed out
                                // and was removed from _inflightRpcs.
                                parserContext.Skip = true;
                            }

                            goto case ParseStep.ReadMainMessageLength;
                        }
                        else
                        {
                            // Not enough data to read header.
                            parserContext.Step = ParseStep.ReadHeader;
                            break;
                        }
                    }
                case ParseStep.ReadMainMessageLength:
                    {
                        if (reader.TryReadVarint(out parserContext.MainMessageLength))
                        {
                            if (parserContext.Skip)
                            {
                                parserContext.RemainingSkipBytes = parserContext.MainMessageLength;
                                goto case ParseStep.Skip;
                            }
                            else
                            {
                                goto case ParseStep.ReadProtobufMessage;
                            }
                        }
                        else
                        {
                            // Not enough data to read main message length.
                            parserContext.Step = ParseStep.ReadMainMessageLength;
                            break;
                        }
                    }
                case ParseStep.ReadProtobufMessage:
                    {
                        var messageLength = parserContext.ProtobufMessageLength;
                        if (reader.Remaining < messageLength)
                        {
                            // Not enough data to parse main protobuf message.
                            parserContext.Step = ParseStep.ReadProtobufMessage;
                            break;
                        }

                        var mainProtobufMessage = reader.Sequence.Slice(
                            reader.Position, messageLength);

                        if (parserContext.Header.IsError)
                        {
                            var error = ProtobufHelper.GetErrorStatus(mainProtobufMessage);
                            var exception = GetException(error);
                            parserContext.Exception = exception;
                        }
                        else
                        {
                            try
                            {
                                parserContext.Rpc.ParseProtobuf(mainProtobufMessage);
                            }
                            catch (Exception ex)
                            {
                                parserContext.Exception = ex;
                            }
                        }

                        reader.Advance(messageLength);

                        if (parserContext.HasSidecars)
                        {
                            parserContext.RemainingSidecarLength = parserContext.SidecarLength;
                            goto case ParseStep.BeginSidecars;
                        }
                        else
                        {
                            return true;
                        }
                    }
                case ParseStep.BeginSidecars:
                    {
                        AdjustSidecarOffsets(parserContext.Header);

                        var sidecars = new KuduSidecarOffsets(
                            parserContext.Header.SidecarOffsets,
                            parserContext.RemainingSidecarLength);

                        parserContext.Rpc.BeginProcessingSidecars(sidecars);
                        parserContext.Step = ParseStep.ReadSidecars;

                        if (parserContext.RemainingSidecarLength == 0)
                            return true;

                        goto case ParseStep.ReadSidecars;
                    }
                case ParseStep.ReadSidecars:
                    {
                        var remaining = reader.Remaining;

                        if (remaining == 0)
                            break;

                        parserContext.Rpc.ParseSidecarSegment(ref reader);
                        remaining -= reader.Remaining;
                        parserContext.RemainingSidecarLength -= (int)remaining;

                        if (parserContext.RemainingSidecarLength == 0)
                            return true;

                        break;
                    }
                case ParseStep.Skip:
                    {
                        var skipBytes = Math.Min(
                            reader.Remaining,
                            parserContext.RemainingSkipBytes);

                        reader.Advance(skipBytes);
                        parserContext.RemainingSkipBytes -= (int)skipBytes;

                        if (parserContext.RemainingSkipBytes == 0)
                            parserContext.Reset();

                        break;
                    }
            }

            return false;
        }

        private Exception GetException(ErrorStatusPB error)
        {
            var code = error.Code;
            KuduException exception;

            if (code == ErrorStatusPB.RpcErrorCodePB.ErrorServerTooBusy ||
                code == ErrorStatusPB.RpcErrorCodePB.ErrorUnavailable)
            {
                exception = new RecoverableException(
                    KuduStatus.ServiceUnavailable(error.Message));
            }
            else if (code == ErrorStatusPB.RpcErrorCodePB.FatalInvalidAuthenticationToken)
            {
                exception = new InvalidAuthnTokenException(
                    KuduStatus.NotAuthorized(error.Message));
            }
            else if (code == ErrorStatusPB.RpcErrorCodePB.ErrorInvalidAuthorizationToken)
            {
                exception = new InvalidAuthzTokenException(
                    KuduStatus.NotAuthorized(error.Message));
            }
            else
            {
                var message = $"{LogPrefix} server sent error {error.Message}";
                exception = new RpcRemoteException(KuduStatus.RemoteError(message), error);
            }

            return exception;
        }

        /// <summary>
        /// Make sidecar offsets zero-based.
        /// </summary>
        /// <param name="header">The header to adjust.</param>
        private void AdjustSidecarOffsets(ResponseHeader header)
        {
            uint[] sidecars = header.SidecarOffsets;
            uint offset = sidecars[0];

            for (int i = 0; i < sidecars.Length; i++)
            {
                sidecars[i] -= offset;
            }
        }

        private bool TryGetRpc(ResponseHeader header, out InflightRpc rpc)
        {
            lock (_inflightRpcs)
            {
                return _inflightRpcs.TryGetValue(header.CallId, out rpc);
            }
        }

        private void CompleteRpc(InflightRpc rpc)
        {
            rpc.TrySetResult(null);
        }

        private void CompleteRpc(InflightRpc rpc, Exception exception)
        {
            rpc.TrySetException(exception);
        }

        private void RemoveInflightRpc(int callId)
        {
            lock (_inflightRpcs)
            {
                _inflightRpcs.Remove(callId);
            }
        }

        /// <summary>
        /// Stops accepting any new RPCs, and completes any outstanding
        /// RPCs with exceptions.
        /// </summary>
        private async Task ShutdownAsync(Exception exception = null)
        {
            await _singleWriter.WaitAsync().ConfigureAwait(false);
            try
            {
                await _ioPipe.Output.CompleteAsync(exception).ConfigureAwait(false);
                await _ioPipe.Input.CompleteAsync(exception).ConfigureAwait(false);
            }
            finally
            {
                _singleWriter.Release();
            }

            if (exception != null)
                _logger.ConnectionDisconnected(_ioPipe.ToString(), exception);

            var closedException = new RecoverableException(KuduStatus.IllegalState(
                $"Connection {_ioPipe} is disconnected."), exception);

            lock (_inflightRpcs)
            {
                _closed = true;
                _closedException = closedException;

                InvokeDisconnectedCallback();

                foreach (var inflightMessage in _inflightRpcs.Values)
                    inflightMessage.TrySetException(closedException);

                _inflightRpcs.Clear();
            }

            (_ioPipe as IDisposable)?.Dispose();
            // TODO: There may still be pending tasks waiting to
            // acquire this semaphore.
            //_singleWriter.Dispose();
        }

        public override string ToString() => _ioPipe.ToString();

        /// <summary>
        /// Stops accepting RPCs and completes any outstanding RPCs with exceptions.
        /// </summary>
        public async Task CloseAsync()
        {
            _ioPipe.Output.CancelPendingFlush();
            await _ioPipe.Output.CompleteAsync().ConfigureAwait(false);

            _ioPipe.Input.CancelPendingRead();
            await _ioPipe.Input.CompleteAsync().ConfigureAwait(false);

            // Wait for the reader loop to finish.
            await _receiveTask.ConfigureAwait(false);
        }

        /// <summary>
        /// The caller must hold the _inflightMessages lock.
        /// </summary>
        private void InvokeDisconnectedCallback()
        {
            try
            {
                _disconnectedCallback.Invoke(_closedException.InnerException);
            }
            catch { }
        }

        private sealed class InflightRpc : TaskCompletionSource<object>
        {
            public KuduRpc Rpc { get; }

            public InflightRpc(KuduRpc rpc)
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
            {
                Rpc = rpc;
            }
        }

        private readonly struct DisconnectedCallback
        {
            private readonly Action<Exception, object> _callback;
            private readonly object _state;

            public DisconnectedCallback(Action<Exception, object> callback, object state)
            {
                _callback = callback;
                _state = state;
            }

            public void Invoke(Exception exception) => _callback?.Invoke(exception, _state);
        }

        private sealed class ParserContext
        {
            public ParseStep Step;

            /// <summary>
            /// Total message length (4 bytes).
            /// </summary>
            public int TotalMessageLength;

            /// <summary>
            /// RPC Header protobuf length (variable encoding).
            /// </summary>
            public int HeaderLength;

            /// <summary>
            /// Main message length (variable encoding).
            /// Includes the size of any sidecars.
            /// </summary>
            public int MainMessageLength;

            /// <summary>
            /// RPC Header protobuf.
            /// </summary>
            public ResponseHeader Header;

            public InflightRpc InflightRpc;

            public Exception Exception;

            /// <summary>
            /// Gets the size of the main message protobuf.
            /// </summary>
            public int ProtobufMessageLength => Header.SidecarOffsets == null ?
                MainMessageLength : (int)Header.SidecarOffsets[0];

            public bool HasSidecars => Header.SidecarOffsets != null;

            public int SidecarLength => MainMessageLength - (int)Header.SidecarOffsets[0];

            public KuduRpc Rpc => InflightRpc.Rpc;

            public int RemainingSidecarLength;

            public bool Skip;

            public int RemainingSkipBytes;

            public void Reset()
            {
                Step = ParseStep.NotStarted;
                TotalMessageLength = default;
                HeaderLength = default;
                MainMessageLength = default;
                Header = default;
                InflightRpc = default;
                Exception = default;
                RemainingSidecarLength = default;
                Skip = default;
                RemainingSkipBytes = default;
            }
        }

        private enum ParseStep : byte
        {
            NotStarted,
            ReadTotalMessageLength,
            ReadHeaderLength,
            ReadHeader,
            ReadMainMessageLength,
            ReadProtobufMessage,
            BeginSidecars,
            ReadSidecars,
            Skip
        }
    }
}
