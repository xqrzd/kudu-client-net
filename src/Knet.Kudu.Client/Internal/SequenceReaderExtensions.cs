using System;
using System.Buffers;

namespace Knet.Kudu.Client.Util
{
    public static class SequenceReaderExtensions
    {
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
