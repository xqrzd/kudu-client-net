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
using Kudu.Client.Util;
using ProtoBuf;
using ProtoBuf.Meta;

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
            var state = new ParseStuff();

            try
            {
                while (true)
                {
                    var result = await input.ReadAsync().ConfigureAwait(false);

                    if (result.IsCanceled || result.IsCompleted)
                        break;

                    await ParseAsync(input, result.Buffer, ref state).ConfigureAwait(false);
                }

                input.Complete();
            }
            catch (Exception ex)
            {
                input.Complete(ex);
                Console.WriteLine("--> Input stopped (Exception) " + _ioPipe.ToString() + ex.ToString());
            }

            // We're done, perform shutdown and mark any outstanding RPC's
            // with exceptions.
            Shutdown();
        }

        private Task ParseAsync(PipeReader input, ReadOnlySequence<byte> buffer, ref ParseStuff state)
        {
            do
            {
                switch (state.ParseState)
                {
                    case ParseState.NotStarted:
                        {
                            if (buffer.TryReadInt32BigEndian(out state.TotalMessageLength))
                                goto case ParseState.ReadHeaderLength;
                            else
                                // Not enough data to read message size.
                                break;
                        }
                    case ParseState.ReadHeaderLength:
                        {
                            if (buffer.TryReadVarintUInt32(out state.HeaderLength))
                                goto case ParseState.ReadHeader;
                            else
                            {
                                // Not enough data to read header length.
                                state.ParseState = ParseState.ReadHeaderLength;
                                break;
                            }
                        }
                    case ParseState.ReadHeader:
                        {
                            if (TryParseResponseHeader(ref buffer, state.HeaderLength, out state.Header))
                                goto case ParseState.ReadMainMessageLength;
                            else
                            {
                                // Not enough data to read header.
                                state.ParseState = ParseState.ReadHeader;
                                break;
                            }
                        }
                    case ParseState.ReadMainMessageLength:
                        {
                            if (buffer.TryReadVarintUInt32(out state.MainMessageLength))
                                goto case ParseState.ReadProtobufMessage;
                            else
                            {
                                // Not enough data to read main message length.
                                state.ParseState = ParseState.ReadMainMessageLength;
                                break;
                            }
                        }
                    case ParseState.ReadProtobufMessage:
                        {
                            var messageLength = state.ProtobufMessageLength;
                            if (buffer.Length < messageLength)
                            {
                                // Not enough data to parse main protobuf message.
                                state.ParseState = ParseState.ReadProtobufMessage;
                                break;
                            }

                            var header = state.Header;
                            var messageProtobuf = buffer.Slice(0, messageLength);
                            var success = TryGetRpc(header, messageProtobuf, out var message);
                            buffer = buffer.Slice(messageLength);

                            if (success)
                            {
                                if (state.HasSidecars)
                                {
                                    var length = state.SidecarLength;

                                    input.AdvanceTo(buffer.Start);
                                    state.Reset();

                                    return ProcessSidecarsAsync(input, message, header, length);
                                }
                                else
                                {
                                    // This request has no sidecars, we're all done.
                                    // Complete this RPC.
                                    message.TrySetResult(null);
                                }
                            }

                            // All done.
                            state.Reset();
                            break;
                        }
                }
            } while (state.ParseState == ParseState.NotStarted && buffer.Length >= 4);

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
                message.TrySetResult(null);
            }
            catch (Exception ex)
            {
                message.TrySetException(ex);
                throw;
            }
        }

        private static bool TryParseResponseHeader(
            ref ReadOnlySequence<byte> buffer, uint length, out ResponseHeader header)
        {
            if (buffer.Length < length)
            {
                header = null;
                return false;
            }

            var slice = buffer.Slice(0, length);
            var reader = ProtoReader.Create(out var state, slice, RuntimeTypeModel.Default);
            var obj = RuntimeTypeModel.Default.Deserialize(reader, ref state, null, typeof(ResponseHeader));
            buffer = buffer.Slice(length);
            header = (ResponseHeader)obj;
            return true;
        }

        private static ErrorStatusPB ParseError(ReadOnlySequence<byte> buffer)
        {
            var reader = ProtoReader.Create(out var state, buffer, RuntimeTypeModel.Default);
            var obj = RuntimeTypeModel.Default.Deserialize(reader, ref state, null, typeof(ErrorStatusPB));
            return (ErrorStatusPB)obj;
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
                var error = ParseError(buffer);
                var exception = new RpcException(error);

                message.TrySetException(exception);
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
                    inflightMessage.TrySetException(closedException);

                _inflightMessages.Clear();
            }

            (_ioPipe as IDisposable)?.Dispose();
            _singleWriter.Dispose();
        }

        public override string ToString() => _ioPipe.ToString();

        /// <summary>
        /// Stops accepting RPCs and completes any outstanding RPCs with exceptions.
        /// This method may be called multiple times, but not concurrently.
        /// </summary>
        public async Task DisposeAsync()
        {
            _ioPipe.Input.CancelPendingRead();
            _ioPipe.Input.Complete();
            _ioPipe.Output.CancelPendingFlush();
            _ioPipe.Output.Complete();

            // Wait for the reader loop to finish.
            await _receiveTask.ConfigureAwait(false);
        }

        private struct ParseStuff
        {
            public ParseState ParseState;

            /// <summary>
            /// Total message length (4 bytes).
            /// </summary>
            public int TotalMessageLength;

            /// <summary>
            /// RPC Header protobuf length (variable encoding).
            /// </summary>
            public uint HeaderLength;

            /// <summary>
            /// Main message length (variable encoding).
            /// Includes the size of any sidecars.
            /// </summary>
            public uint MainMessageLength;

            /// <summary>
            /// RPC Header protobuf.
            /// </summary>
            public ResponseHeader Header;

            /// <summary>
            /// Gets the size of the main message protobuf.
            /// </summary>
            public int ProtobufMessageLength => Header.SidecarOffsets == null ?
                (int)MainMessageLength : (int)Header.SidecarOffsets[0];

            public bool HasSidecars => Header.SidecarOffsets != null;

            public int SidecarLength => (int)MainMessageLength - (int)Header.SidecarOffsets[0];

            public void Reset()
            {
                ParseState = ParseState.NotStarted;
                TotalMessageLength = 0;
                HeaderLength = 0;
                MainMessageLength = 0;
                Header = null;
            }
        }

        private enum ParseState
        {
            NotStarted,
            ReadTotalMessageLength,
            ReadHeaderLength,
            ReadHeader,
            ReadMainMessageLength,
            ReadProtobufMessage,
            ReadSidecars
        }

        private sealed class InflightMessage : TaskCompletionSource<object>
        {
            public KuduRpc Rpc { get; }

            public InflightMessage(KuduRpc rpc)
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
            {
                Rpc = rpc;
            }
        }
    }
}
