using System;
using System.Buffers;
using System.Diagnostics;
using System.Text;

namespace Knet.Kudu.Client.Util
{
    public static class SequenceReaderExtensions
    {
        public static bool TryReadUtf8String(
            ref this SequenceReader<byte> reader, int length, out string value)
        {
            ReadOnlySpan<byte> span = reader.UnreadSpan;
            if (span.Length < length)
                return TryReadMultisegmentUtf8String(ref reader, length, out value);

            ReadOnlySpan<byte> slice = span.Slice(0, length);
            value = Encoding.UTF8.GetString(slice);
            reader.Advance(length);
            return true;
        }

        private static bool TryReadMultisegmentUtf8String(
            ref SequenceReader<byte> reader, int length, out string value)
        {
            Debug.Assert(reader.UnreadSpan.Length < length);

            // Not enough data in the current segment, try to peek for the data we need.
            // Kudu strings are limited to 64KB, so using stack memory should be fine.
            Span<byte> tempSpan = stackalloc byte[length];

            if (!reader.TryCopyTo(tempSpan))
            {
                value = default;
                return false;
            }

            value = Encoding.UTF8.GetString(tempSpan);
            reader.Advance(length);
            return true;
        }

        public static bool TryReadVarint(this ref SequenceReader<byte> reader, out int value)
        {
            value = 0;

            for (int i = 0; i < 4; i++)
            {
                if (reader.TryRead(out byte chunk))
                {
                    value |= (chunk & 0x7F) << 7 * i;
                    if ((chunk & 0x80) == 0)
                        return true;
                }
                else
                {
                    reader.Rewind(i);
                    return false;
                }
            }

            if (reader.TryRead(out byte b1))
            {
                value |= b1 << 28; // Can only use 4 bits from this chunk.
                if ((b1 & 0xF0) == 0)
                    return true;
            }
            else
            {
                reader.Rewind(4);
                return false;
            }

            throw new OverflowException("Error decoding varint32");
        }
    }
}
