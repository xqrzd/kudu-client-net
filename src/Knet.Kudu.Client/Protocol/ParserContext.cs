using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf.Rpc;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Protocol
{
    internal sealed class ParserContext : IDisposable
    {
        private const int MaxRpcSize = 256 * 1024 * 1024; // 256MB

        public ParseStep Step;

        public int TotalMessageLength;

        public int HeaderLength;

        public ResponseHeader Header;

        /// <summary>
        /// Length of the main message, including sidecars.
        /// </summary>
        public int MainMessageLength;

        /// <summary>
        /// For optimal performance, the reader should wait until this
        /// many bytes are available before attempting to parse the
        /// message again.
        /// </summary>
        public int ReadHint;

        public Memory<byte> RemainingMainMessage;

        public KuduMessage Message = new();

        public void Dispose() => Reset();

        public void Reset()
        {
            Step = ParseStep.TotalMessageLength;
            TotalMessageLength = 0;
            HeaderLength = 0;
            Header = null;
            MainMessageLength = 0;
            ReadHint = 0;
            RemainingMainMessage = default;
            Message.Reset();
        }

        public bool TryReadTotalMessageLength(ref SequenceReader<byte> reader)
        {
            var success = reader.TryReadBigEndian(out TotalMessageLength);

            if (success && TotalMessageLength > MaxRpcSize)
            {
                ThrowRpcTooLongException(TotalMessageLength);
            }

            return success;
        }

        public bool TryReadHeaderLength(ref SequenceReader<byte> reader)
        {
            return reader.TryReadVarint(out HeaderLength);
        }

        public bool TryReadMessageLength(ref SequenceReader<byte> reader)
        {
            return reader.TryReadVarint(out MainMessageLength);
        }

        public bool TryReadResponseHeader(ref SequenceReader<byte> reader)
        {
            var length = HeaderLength;

            if (reader.Remaining < length)
            {
                return false;
            }

            var slice = reader.Sequence.Slice(reader.Position, length);
            Header = ResponseHeader.Parser.ParseFrom(slice);

            reader.Advance(length);

            return true;
        }

        public bool TryReadMainMessage(ref SequenceReader<byte> reader)
        {
            if (RemainingMainMessage.Length == 0)
            {
                InitializeMessage();
            }

            var desiredLength = RemainingMainMessage.Length;
            var availableLength = (int)reader.Remaining;

            var readLength = Math.Min(desiredLength, availableLength);
            var slice = RemainingMainMessage.Span.Slice(0, readLength);

            reader.TryCopyTo(slice);
            reader.Advance(readLength);

            RemainingMainMessage = RemainingMainMessage.Slice(readLength);

            if (RemainingMainMessage.Length > 0)
            {
                ReadHint = RemainingMainMessage.Length;
                return false;
            }

            return true;
        }

        private void InitializeMessage()
        {
            var rawSidecarOffsets = Header.SidecarOffsets;
            var messageProtobufLength = rawSidecarOffsets.Count == 0
                ? MainMessageLength
                : (int)rawSidecarOffsets[0];

            var sidecarOffsets = GetSidecarOffsets(rawSidecarOffsets, MainMessageLength);
            var buffer = new ArrayPoolBuffer<byte>(MainMessageLength);

            Message.Init(buffer, messageProtobufLength, sidecarOffsets);
            RemainingMainMessage = buffer.Buffer.AsMemory(0, MainMessageLength);
        }

        private static SidecarOffset[] GetSidecarOffsets(
            RepeatedField<uint> rawOffsets, int mainMessageLength)
        {
            int numSidecars = rawOffsets.Count;

            if (numSidecars <= 0)
            {
                return Array.Empty<SidecarOffset>();
            }

            var sidecarOffsets = new SidecarOffset[numSidecars];

            for (int i = 0; i < numSidecars - 1; i++)
            {
                var currentOffset = rawOffsets[i];
                var nextOffset = rawOffsets[i + 1];
                var size = nextOffset - currentOffset;

                var sidecarOffset = new SidecarOffset((int)currentOffset, (int)size);
                sidecarOffsets[i] = sidecarOffset;
            }

            // Handle the last sidecar.
            var lastOffset = (int)rawOffsets[numSidecars - 1];
            var lastLength = mainMessageLength - lastOffset;
            var lastSidecarOffset = new SidecarOffset(lastOffset, lastLength);
            sidecarOffsets[numSidecars - 1] = lastSidecarOffset;

            return sidecarOffsets;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowRpcTooLongException(int totalMessageLength)
        {
            var status = KuduStatus.IllegalState(
                $"Received RPC is too long: {totalMessageLength}. Max length: {MaxRpcSize}");

            throw new NonRecoverableException(status);
        }
    }
}
