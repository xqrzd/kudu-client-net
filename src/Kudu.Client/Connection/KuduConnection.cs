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
        private readonly Dictionary<int, InflightMessage> _inflightMessages;
        private readonly Task _receiveTask;

        private int _nextCallId;
        private bool _closed;
        private Exception _closedException;

        public KuduConnection(IDuplexPipe ioPipe)
        {
            _ioPipe = ioPipe;
            _singleWriter = new SemaphoreSlim(1, 1);
            _inflightMessages = new Dictionary<int, InflightMessage>();
            _nextCallId = 0;

            _receiveTask = ReceiveAsync();
        }

        public void OnDisconnected(Action<Exception, object> callback, object state)
        {
            _ioPipe.Input.OnWriterCompleted(callback, state);
        }

        public async Task SendReceiveAsync(RequestHeader header, KuduRpc rpc)
        {
            var message = new InflightMessage(rpc);
            lock (_inflightMessages)
            {
                if (_closed)
                {
                    // This connection is closed. The RPC needs to be retried
                    // on another connection.
                    throw _closedException;
                }

                header.CallId = _nextCallId++;

                _inflightMessages.Add(header.CallId, message);
            }
            await SendAsync(header, rpc).ConfigureAwait(false);
            await message.Task.ConfigureAwait(false);
        }

        private async ValueTask<FlushResult> SendAsync(RequestHeader header, KuduRpc rpc)
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

                return await WriteAsync(stream.AsMemory()).ConfigureAwait(false);
            }
        }

        private async ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> source)
        {
            // TODO: CancellationToken support.
            PipeWriter output = _ioPipe.Output;
            await _singleWriter.WaitAsync().ConfigureAwait(false);
            try
            {
                return await output.WriteAsync(source).ConfigureAwait(false);
            }
            finally
            {
                _singleWriter.Release();
            }
        }

        private async Task ReceiveAsync()
        {
            PipeReader input = _ioPipe.Input;
            var parserContext = new ParserContext();

            try
            {
                while (true)
                {
                    var result = await input.ReadAsync().ConfigureAwait(false);

                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    await ParseAsync(input, result.Buffer, parserContext).ConfigureAwait(false);
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

        private Task ParseAsync(
            PipeReader input, ReadOnlySequence<byte> buffer, ParserContext parserContext)
        {
            do
            {
                if (KuduProtocol.TryParseMessage(ref buffer, parserContext))
                {
                    var header = parserContext.Header;
                    var messageLength = parserContext.ProtobufMessageLength;
                    var messageProtobuf = buffer.Slice(0, messageLength);
                    var success = TryGetRpc(header, messageProtobuf, out var message);
                    buffer = buffer.Slice(messageLength);

                    if (success)
                    {
                        if (parserContext.HasSidecars)
                        {
                            input.AdvanceTo(buffer.Start);

                            var length = parserContext.SidecarLength;
                            return ProcessSidecarsAsync(input, message, header, length);
                        }
                        else
                        {
                            message.Complete();
                        }
                    }
                }
            } while (parserContext.Step == ParseStep.NotStarted && buffer.Length >= 4);

            input.AdvanceTo(buffer.Start, buffer.End);

            return Task.CompletedTask;
        }

        private async Task ProcessSidecarsAsync(
            PipeReader input,
            InflightMessage message,
            ResponseHeader header,
            int length)
        {
            try
            {
                await message.Rpc.ParseSidecarsAsync(header, input, length).ConfigureAwait(false);
                message.Complete();
            }
            catch (Exception ex)
            {
                // We've already dequeued the message from _inflightMessages,
                // so we can't rely on the connection shutdown to mark this
                // rpc as failed.
                message.CompleteWithError(ex);
                throw;
            }
        }

        private bool TryGetRpc(ResponseHeader header, ReadOnlySequence<byte> buffer, out InflightMessage message)
        {
            bool success;

            lock (_inflightMessages)
            {
                success = _inflightMessages.Remove(header.CallId, out message);
            }

            if (!success)
            {
                throw new Exception($"Unable to find inflight message {header.CallId}");
            }

            if (header.IsError)
            {
                var error = KuduProtocol.ParseError(buffer);
                var exception = new RpcException(error);

                message.CompleteWithError(exception);
                return false;
            }
            else
            {
                message.Rpc.ParseProtobuf(buffer);
                return true;
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

            lock (_inflightMessages)
            {
                _closed = true;
                _closedException = closedException;

                foreach (var inflightMessage in _inflightMessages.Values)
                    inflightMessage.CompleteWithError(closedException);

                _inflightMessages.Clear();
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

        private sealed class InflightMessage : TaskCompletionSource<KuduRpc>
        {
            public KuduRpc Rpc { get; }

            public InflightMessage(KuduRpc rpc)
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
            {
                Rpc = rpc;
            }

            public void Complete() => TrySetResult(Rpc);

            public void CompleteWithError(Exception exception) => TrySetException(exception);
        }
    }
}
