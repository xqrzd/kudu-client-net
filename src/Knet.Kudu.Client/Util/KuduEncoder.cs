using System;
using System.Buffers.Binary;
using System.Text;

namespace Knet.Kudu.Client.Util
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

        public static void EncodeInt128(Span<byte> destination, KuduInt128 value)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(destination, value.Low);
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(8), value.High);
        }

        public static void EncodeDateTime(Span<byte> destination, DateTime value)
        {
            long micros = EpochTime.ToUnixTimeMicros(value);
            EncodeInt64(destination, micros);
        }

        public static void EncodeDate(Span<byte> destination, DateTime value)
        {
            int days = EpochTime.ToUnixTimeDays(value);
            EncodeInt32(destination, days);
        }

        public static void EncodeFloat(Span<byte> destination, float value)
        {
            int intValue = value.AsInt();
            EncodeInt32(destination, intValue);
        }

        public static void EncodeDouble(Span<byte> destination, double value)
        {
            long longValue = value.AsLong();
            EncodeInt64(destination, longValue);
        }

        public static void EncodeDecimal32(
            Span<byte> destination, decimal value, int precision, int scale)
        {
            int encodedValue = DecimalUtil.EncodeDecimal32(value, precision, scale);
            EncodeInt32(destination, encodedValue);
        }

        public static void EncodeDecimal64(
            Span<byte> destination, decimal value, int precision, int scale)
        {
            long encodedValue = DecimalUtil.EncodeDecimal64(value, precision, scale);
            EncodeInt64(destination, encodedValue);
        }

        public static void EncodeDecimal128(
            Span<byte> destination, decimal value, int precision, int scale)
        {
            var encodedValue = DecimalUtil.EncodeDecimal128(value, precision, scale);
            EncodeInt128(destination, encodedValue);
        }

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

        public static byte[] EncodeInt128(KuduInt128 value)
        {
            var buffer = new byte[16];
            EncodeInt128(buffer, value);
            return buffer;
        }

        public static byte[] EncodeDateTime(DateTime value)
        {
            var buffer = new byte[8];
            EncodeDateTime(buffer, value);
            return buffer;
        }

        public static byte[] EncodeDate(DateTime value)
        {
            var buffer = new byte[8];
            EncodeDate(buffer, value);
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

        public static byte[] EncodeDecimal32(decimal value, int precision, int scale)
        {
            int intVal = DecimalUtil.EncodeDecimal32(value, precision, scale);
            return EncodeInt32(intVal);
        }

        public static byte[] EncodeDecimal64(decimal value, int precision, int scale)
        {
            long longVal = DecimalUtil.EncodeDecimal64(value, precision, scale);
            return EncodeInt64(longVal);
        }

        public static byte[] EncodeDecimal128(decimal value, int precision, int scale)
        {
            var intVal = DecimalUtil.EncodeDecimal128(value, precision, scale);
            return EncodeInt128(intVal);
        }

        public static byte[] EncodeString(string source) =>
            Encoding.UTF8.GetBytes(source);

        public static byte[] EncodeDefaultValue(KuduType type, object value)
        {
            return type switch
            {
                KuduType.Int8 => EncodeInt8((sbyte)value),
                KuduType.Int16 => EncodeInt16((short)value),
                KuduType.Int32 => EncodeInt32((int)value),
                KuduType.Int64 => EncodeInt64((long)value),
                KuduType.String => EncodeString((string)value),
                KuduType.Bool => EncodeBool((bool)value),
                KuduType.Float => EncodeFloat((float)value),
                KuduType.Double => EncodeDouble((double)value),
                KuduType.Binary => (byte[])value,
                KuduType.UnixtimeMicros => EncodeDateTime((DateTime)value),
                KuduType.Date => EncodeDate((DateTime)value),
                KuduType.Decimal32 => EncodeDefaultDecimal((decimal)value),
                KuduType.Decimal64 => EncodeDefaultDecimal((decimal)value),
                KuduType.Decimal128 => EncodeDefaultDecimal((decimal)value),
                _ => throw new Exception($"Unknown data type {type}"),
            };
        }

        /// <summary>
        /// Serializes an object based on its C# type. Used for Alter Column
        /// operations where the column's type is not available.
        /// </summary>
        /// <param name="value">The value to serialize.</param>
        public static byte[] EncodeDefaultValue(object value)
        {
            return value switch
            {
                sbyte v => EncodeInt8(v),
                byte v => EncodeInt8((sbyte)v),
                short v => EncodeInt16(v),
                ushort v => EncodeInt16((short)v),
                int v => EncodeInt32(v),
                uint v => EncodeInt32((int)v),
                long v => EncodeInt64(v),
                ulong v => EncodeInt64((long)v),
                string v => EncodeString(v),
                bool v => EncodeBool(v),
                float v => EncodeFloat(v),
                double v => EncodeDouble(v),
                byte[] v => v,
                DateTime v => EncodeDateTime(v),
                // TODO: Add Date support here. For that, we need the data type.
                decimal v => EncodeDefaultDecimal(v),
                _ => throw new Exception($"Unsupported data type {value.GetType().Name}"),
            };
        }

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

        public static KuduInt128 DecodeInt128(ReadOnlySpan<byte> source)
        {
            var low = BinaryPrimitives.ReadUInt64LittleEndian(source);
            var high = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(8));

            return new KuduInt128(high, low);
        }

        public static DateTime DecodeDateTime(ReadOnlySpan<byte> source)
        {
            long micros = DecodeInt64(source);
            return EpochTime.FromUnixTimeMicros(micros);
        }

        public static DateTime DecodeDate(ReadOnlySpan<byte> source)
        {
            int days = DecodeInt32(source);
            return EpochTime.FromUnixTimeDays(days);
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

        public static decimal DecodeDecimal(ReadOnlySpan<byte> source, KuduType kuduType, int scale)
        {
            switch (kuduType)
            {
                case KuduType.Decimal32:
                    int intVal = DecodeInt32(source);
                    return DecimalUtil.DecodeDecimal32(intVal, scale);

                case KuduType.Decimal64:
                    long longVal = DecodeInt64(source);
                    return DecimalUtil.DecodeDecimal64(longVal, scale);

                case KuduType.Decimal128:
                    KuduInt128 int128Val = DecodeInt128(source);
                    return DecimalUtil.DecodeDecimal128(int128Val, scale);

                default:
                    throw new Exception($"Unsupported data type: {kuduType}.");
            }
        }

        public static string DecodeString(ReadOnlySpan<byte> source) =>
            Encoding.UTF8.GetString(source);

        public static int BitsToBytes(int bits) => (int)(((uint)bits + 7) / 8);

        /// <summary>
        /// This method will apply xor on the left most bit of the first byte in
        /// the buffer. This is used in Kudu to have unsigned data types sorting
        /// correctly.
        /// </summary>
        /// <param name="buffer">Buffer whose left most bit will be xor'd.</param>
        public static byte XorLeftMostBit(Span<byte> buffer) => buffer[0] ^= 1 << 7;

        private static byte[] EncodeDefaultDecimal(decimal value)
        {
            var scale = DecimalUtil.GetScale(value);

            return EncodeDecimal128(
                value, DecimalUtil.MaxDecimal128Precision, scale);
        }
    }
}
