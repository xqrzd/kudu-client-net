using System;
using System.Buffers.Binary;
using System.Text;

namespace Kudu.Client.Util
{
    public static class KuduEncoder
    {
        public static void EncodeBool(Span<byte> destination, bool value) =>
            destination[0] = (byte)(value ? 1 : 0);

        public static void EncodeInt8(Span<byte> destination, sbyte value) =>
            destination[0] = (byte)value;

        public static void EncodeInt16(Span<byte> destination, short value) =>
            BinaryPrimitives.WriteInt16LittleEndian(destination, value);

        public static void EncodeInt32(Span<byte> destination, int value) =>
            BinaryPrimitives.WriteInt32LittleEndian(destination, value);

        public static void EncodeInt64(Span<byte> destination, long value) =>
            BinaryPrimitives.WriteInt64LittleEndian(destination, value);

        public static byte[] EncodeBool(bool value) =>
            value ? new byte[] { 1 } : new byte[] { 0 };

        public static byte[] EncodeInt8(sbyte value) =>
            new byte[] { (byte)value };

        public static byte[] EncodeInt16(short value)
        {
            var buffer = new byte[2];
            EncodeInt16(buffer, value);
            return buffer;
        }

        public static byte[] EncodeInt32(int value)
        {
            var buffer = new byte[4];
            EncodeInt32(buffer, value);
            return buffer;
        }

        public static byte[] EncodeInt64(long value)
        {
            var buffer = new byte[8];
            EncodeInt64(buffer, value);
            return buffer;
        }

        public static byte[] EncodeFloat(float value)
        {
            int bits = value.AsInt();
            return EncodeInt32(bits);
        }

        public static byte[] EncodeDouble(double value)
        {
            long bits = value.AsLong();
            return EncodeInt64(bits);
        }

        public static byte[] EncodeString(string source) =>
            Encoding.UTF8.GetBytes(source);

        public static bool DecodeBool(ReadOnlySpan<byte> source) =>
            source[0] > 0;

        public static sbyte DecodeInt8(ReadOnlySpan<byte> source) =>
            (sbyte)source[0];

        public static short DecodeInt16(ReadOnlySpan<byte> source) =>
            BinaryPrimitives.ReadInt16LittleEndian(source);

        public static int DecodeInt32(ReadOnlySpan<byte> source) =>
            BinaryPrimitives.ReadInt32LittleEndian(source);

        public static long DecodeInt64(ReadOnlySpan<byte> source) =>
            BinaryPrimitives.ReadInt64LittleEndian(source);

        public static DateTime DecodeTimestamp(ReadOnlySpan<byte> source)
        {
            long micros = DecodeInt64(source);
            return EpochTime.FromUnixEpochMicros(micros);
        }

        public static float DecodeFloat(ReadOnlySpan<byte> source)
        {
            int value = DecodeInt32(source);
            return value.AsFloat();
        }

        public static double DecodeDouble(ReadOnlySpan<byte> source)
        {
            long value = DecodeInt64(source);
            return value.AsDouble();
        }

        public static string DecodeString(ReadOnlySpan<byte> source) =>
            Encoding.UTF8.GetString(source);
    }
}
