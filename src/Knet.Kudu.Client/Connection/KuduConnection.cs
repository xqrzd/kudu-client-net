﻿using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protocol.Rpc;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Util;
using ProtoBuf;

namespace Knet.Kudu.Client.Connection
{
    public class KuduConnection
    {
        private readonly IDuplexPipe _ioPipe;
        private readonly SemaphoreSlim _singleWriter;
        private readonly Dictionary<int, InflightRpc> _inflightRpcs;
        private readonly Task _receiveTask;

        private int _nextCallId;
        private bool _closed;
        private RecoverableException _closedException;
        private DisconnectedCallback _disconnectedCallback;

        public KuduConnection(IDuplexPipe ioPipe)
        {
            _ioPipe = ioPipe;
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

        public async Task<T> SendReceiveAsync<T>(RequestHeader header, KuduRpc<T> rpc)
        {
            var message = new InflightRpc(rpc);

            lock (_inflightRpcs)
            {
                if (_closed)
                {
                    // The upper-level caller should handle the exception
                    // and retry using a new connection.
                    throw _closedException;
                }

                header.CallId = _nextCallId++;

                _inflightRpcs.Add(header.CallId, message);
            }

            using (rpc)
            {
                await SendAsync(header, rpc).ConfigureAwait(false);
                await message.Task.ConfigureAwait(false);

                return rpc.Output;
            }
        }

        private async ValueTask SendAsync(RequestHeader header, KuduRpc rpc)
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

                await WriteAsync(stream.AsMemory()).ConfigureAwait(false);
            }
        }

        private async ValueTask WriteAsync(ReadOnlyMemory<byte> source)
        {
            // TODO: CancellationToken support.
            PipeWriter output = _ioPipe.Output;
            await _singleWriter.WaitAsync().ConfigureAwait(false);
            try
            {
                await output.WriteAsync(source).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new RecoverableException(KuduStatus.IllegalState(
                    $"Connection {_ioPipe} is disconnected."), ex);
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

                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    ParseMessages(buffer, parserContext, out var consumed);

                    input.AdvanceTo(consumed, buffer.End);
                }

                input.Complete();
                Shutdown();
            }
            catch (Exception ex)
            {
                input.Complete(ex);
                Shutdown(ex);
                Console.WriteLine("--> Input stopped (Exception) " + _ioPipe.ToString() + ex.ToString());
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
                    var header = parserContext.Header;
                    var callId = header.CallId;

                    if (header.IsError)
                    {
                        var exception = GetException(parserContext.Error);
                        CompleteRpc(callId, exception);
                    }
                    else
                    {
                        CompleteRpc(callId);
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
                        if (TryParseResponseHeader(ref reader,
                            parserContext.HeaderLength, out parserContext.Header))
                        {
                            parserContext.Rpc = GetRpc(parserContext.Header).Rpc;
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
                            goto case ParseStep.ReadProtobufMessage;
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
                            parserContext.Error = GetRpcError(mainProtobufMessage);
                        }
                        else
                        {
                            parserContext.Rpc.ParseProtobuf(mainProtobufMessage);
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
                        {
                            return true;
                        }

                        break;
                    }
            }

            return false;
        }

        private static bool TryParseResponseHeader(
            ref SequenceReader<byte> reader, long length, out ResponseHeader header)
        {
            if (reader.Remaining < length)
            {
                header = null;
                return false;
            }

            var slice = reader.Sequence.Slice(reader.Position, length);
            header = Serializer.Deserialize<ResponseHeader>(slice);

            reader.Advance(length);

            return true;
        }

        private static ErrorStatusPB GetRpcError(ReadOnlySequence<byte> buffer)
        {
            return Serializer.Deserialize<ErrorStatusPB>(buffer);
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

        private InflightRpc GetRpc(ResponseHeader header)
        {
            bool success;
            InflightRpc rpc;

            lock (_inflightRpcs)
            {
                success = _inflightRpcs.TryGetValue(header.CallId, out rpc);
            }

            if (!success)
            {
                // If we get a bad RPC ID back, we are probably somehow misaligned from
                // the server. So, we disconnect the connection.

                throw new NonRecoverableException(KuduStatus.IllegalState(
                    $"{LogPrefix} invalid callID: {header.CallId}"));
            }

            return rpc;
        }

        private void CompleteRpc(int callId)
        {
            if (TryRemoveRpc(callId, out var rpc))
            {
                rpc.TrySetResult(null);
            }
        }

        private void CompleteRpc(int callId, Exception exception)
        {
            if (TryRemoveRpc(callId, out var rpc))
            {
                rpc.TrySetException(exception);
            }
        }

        private bool TryRemoveRpc(int callId, out InflightRpc rpc)
        {
            lock (_inflightRpcs)
            {
                return _inflightRpcs.Remove(callId, out rpc);
            }
        }

        /// <summary>
        /// Stops accepting any new RPCs, and completes any outstanding
        /// RPCs with exceptions.
        /// </summary>
        /// <param name="exception"></param>
        private void Shutdown(Exception exception = null)
        {
            var closedException = new RecoverableException(KuduStatus.IllegalState(
                $"Connection {_ioPipe} is disconnected."), exception);

            lock (_inflightRpcs)
            {
                _closed = true;
                _closedException = closedException;

                foreach (var inflightMessage in _inflightRpcs.Values)
                    inflightMessage.TrySetException(closedException);

                InvokeDisconnectedCallback();

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
        public Task StopAsync()
        {
            _ioPipe.Input.CancelPendingRead();
            _ioPipe.Input.Complete();
            _ioPipe.Output.CancelPendingFlush();
            _ioPipe.Output.Complete();

            // Wait for the reader loop to finish.
            return _receiveTask;
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
    }
}