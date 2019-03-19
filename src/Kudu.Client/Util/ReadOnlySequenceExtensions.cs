using System;
using System.Buffers;
using System.Buffers.Binary;

namespace Kudu.Client.Util
{
    public static class ReadOnlySequenceExtensions
    {
        public static bool TryReadInt32BigEndian(
            this ref ReadOnlySequence<byte> buffer, out int value)
        {
            ReadOnlySpan<byte> span = buffer.First.Span;
            if (span.Length < 4)
                return TryReadMultisegment(ref buffer, out value);

            value = BinaryPrimitives.ReadInt32BigEndian(span);
            buffer = buffer.Slice(4);
            return true;
        }

        private static unsafe bool TryReadMultisegment(
            this ref ReadOnlySequence<byte> buffer, out int value)
        {
            if (buffer.Length < 4)
            {
                value = default;
                return false;
            }

            // Not enough data in the current segment.
            Span<byte> tempSpan = stackalloc byte[4];
            buffer.Slice(0, 4).CopyTo(tempSpan);

            value = BinaryPrimitives.ReadInt32BigEndian(tempSpan);
            buffer = buffer.Slice(4);
            return true;
        }

        public static bool TryReadVarintUInt32(
            this ref ReadOnlySequence<byte> buffer, out uint value)
        {
            ReadOnlySpan<byte> span = buffer.First.Span;
            if (span.Length <= 5)
                return TryReadMultisegmentVarintUInt32(ref buffer, out value);

            ReadOnlySpan<byte> slice = span.Slice(0, 5);
            int length = ParseVarintUInt32(slice, out value);
            buffer = buffer.Slice(length);
            return true;
        }

        private static bool TryReadMultisegmentVarintUInt32(
            ref ReadOnlySequence<byte> buffer, out uint value)
        {
            int length = Math.Min(5, (int)buffer.Length);
            Span<byte> tempSpan = stackalloc byte[length];
            buffer.Slice(0, length).CopyTo(tempSpan);

            try
            {
                // This try-catch is terrible, but this method shouldn't
                // be called often. Eventually this should be improved.
                int read = ParseVarintUInt32(tempSpan, out value);
                buffer = buffer.Slice(read);
                return true;
            }
            catch (IndexOutOfRangeException)
            {
                // This buffer doesn't have the entire integer.
                value = default;
                return false;
            }
        }

        private static int ParseVarintUInt32(ReadOnlySpan<byte> span, out uint value)
        {
            value = span[0];
            return (value & 0x80) == 0 ? 1 : ParseVarintUInt32Tail(span, ref value);
        }

        private static int ParseVarintUInt32Tail(ReadOnlySpan<byte> span, ref uint value)
        {
            uint chunk = span[1];
            value = (value & 0x7F) | (chunk & 0x7F) << 7;
            if ((chunk & 0x80) == 0) return 2;

            chunk = span[2];
            value |= (chunk & 0x7F) << 14;
            if ((chunk & 0x80) == 0) return 3;

            chunk = span[3];
            value |= (chunk & 0x7F) << 21;
            if ((chunk & 0x80) == 0) return 4;

            chunk = span[4];
            value |= chunk << 28; // can only use 4 bits from this chunk
            if ((chunk & 0xF0) == 0) return 5;

            throw new OverflowException("Error decoding varint32");
        }
    }
}
