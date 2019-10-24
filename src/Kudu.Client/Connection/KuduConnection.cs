using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Kudu.Client.Exceptions;
using Kudu.Client.Internal;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Requests;
using ProtoBuf;

namespace Kudu.Client.Connection
{
    public class KuduConnection
    {
        private readonly IDuplexPipe _ioPipe;
        private readonly SemaphoreSlim _singleWriter;
        private readonly Dictionary<int, InflightRpc> _inflightRpcs;
        private readonly Task _receiveTask;

        private int _nextCallId;
        private bool _closed;
        private Exception _closedException;
        private DisconnectedCallback _disconnectedCallback;

        public KuduConnection(IDuplexPipe ioPipe)
        {
            _ioPipe = ioPipe;
            _singleWriter = new SemaphoreSlim(1, 1);
            _inflightRpcs = new Dictionary<int, InflightRpc>();
            _nextCallId = 0;

            _receiveTask = ReceiveAsync();
        }

        public void OnDisconnected(Action<Exception, object> callback, object state)
        {
            lock (_inflightRpcs)
            {
                _disconnectedCallback = new DisconnectedCallback(callback, state);

                if (_closed)
                {
                    // Guarantee the disconnected callback is invoked if
                    // the connection is already closed.
                    InvokeDisconnectedCallback(_closedException);
                }
            }
        }

        public async Task SendReceiveAsync(RequestHeader header, KuduRpc rpc)
        {
            var message = new InflightRpc(rpc);

            lock (_inflightRpcs)
            {
                if (_closed)
                {
                    // The upper-level caller should handle the exception
                    // and retry using a new connection.
                    throw new RecoverableException(
                        KuduStatus.IllegalState("Connection is disconnected."), _closedException);
                }

                header.CallId = _nextCallId++;

                _inflightRpcs.Add(header.CallId, message);
            }

            await SendAsync(header, rpc).ConfigureAwait(false);

            await message.Task.ConfigureAwait(false);
        }

        private async ValueTask SendAsync(RequestHeader header, KuduRpc rpc)
        {
            // TODO: Use PipeWriter once protobuf-net supports it.
            using (var stream = new RecyclableMemoryStream())
            {
                // Make space to write the length of the entire message.
                stream.GetMemory(4);

                Serializer.SerializeWithLengthPrefix(stream, header, PrefixStyle.Base128);
                rpc.WriteRequest(stream);

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

                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    var buffer = result.Buffer;

                    await ParseAsync(input, buffer, parserContext).ConfigureAwait(false);
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

        private Task ParseAsync(PipeReader input, ReadOnlySequence<byte> buffer, ParserContext parserContext)
        {
            var reader = new SequenceReader<byte>(buffer);

            do
            {
                if (KuduProtocol.TryParseMessage(ref reader, parserContext))
                {
                    var header = parserContext.Header;
                    var callId = header.CallId;
                    var rpc = GetRpc(header);

                    if (header.IsError)
                    {
                        var exception = KuduProtocol.GetRpcError(parserContext);
                        CompleteRpc(callId, exception);
                    }
                    else
                    {
                        rpc.Rpc.ParseProtobuf(parserContext.MainProtobufMessage);

                        if (parserContext.HasSidecars)
                        {
                            input.AdvanceTo(reader.Position);

                            var length = parserContext.SidecarLength;
                            return ProcessSidecarsAsync(input, rpc, header, length);
                        }
                        else
                        {
                            CompleteRpc(callId);
                        }
                    }
                }
            } while (parserContext.Step == ParseStep.NotStarted && reader.Remaining >= 4);

            input.AdvanceTo(reader.Position, buffer.End);

            return Task.CompletedTask;
        }

        private async Task ProcessSidecarsAsync(
            PipeReader input,
            InflightRpc rpc,
            ResponseHeader header,
            int length)
        {
            try
            {
                AdjustSidecarOffsets(header);
                await rpc.Rpc.ParseSidecarsAsync(header, input, length).ConfigureAwait(false);
                CompleteRpc(header.CallId);
            }
            catch (Exception ex)
            {
                CompleteRpc(header.CallId, ex);

                // TODO: We probably don't need to burn this entire connection
                // just because we had an error parsing sidecars.

                // Ideally we could just read any remaining data and continue.
                throw;
            }
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
                    $"[peer {_ioPipe}] invalid callID: {header.CallId}"));
            }

            return rpc;
        }

        private void CompleteRpc(int callId)
        {
            bool success;
            InflightRpc rpc;

            lock (_inflightRpcs)
            {
                success = _inflightRpcs.Remove(callId, out rpc);
            }

            if (success)
            {
                rpc.TrySetResult(null);
            }
        }

        private void CompleteRpc(int callId, Exception exception)
        {
            bool success;
            InflightRpc rpc;

            lock (_inflightRpcs)
            {
                success = _inflightRpcs.Remove(callId, out rpc);
            }

            if (success)
            {
                rpc.TrySetException(exception);
            }
        }

        /// <summary>
        /// Stops accepting any new RPCs, and completes any outstanding
        /// RPCs with exceptions.
        /// </summary>
        /// <param name="exception"></param>
        private void Shutdown(Exception exception = null)
        {
            var closedException = new ConnectionClosedException(_ioPipe.ToString(), exception);

            lock (_inflightRpcs)
            {
                _closed = true;
                _closedException = closedException;

                foreach (var inflightMessage in _inflightRpcs.Values)
                    inflightMessage.TrySetException(closedException);

                InvokeDisconnectedCallback(closedException);

                _inflightRpcs.Clear();
            }

            (_ioPipe as IDisposable)?.Dispose();
            _singleWriter.Dispose();
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
        private void InvokeDisconnectedCallback(Exception exception)
        {
            try { _disconnectedCallback.Invoke(exception); } catch { }
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
