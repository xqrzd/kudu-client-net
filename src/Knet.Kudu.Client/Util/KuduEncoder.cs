using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace Knet.Kudu.Client.Util
{
    public static class KuduEncoder
    {
        public static void EncodeBool(Span<byte> destination, bool value) =>
            destination[0] = (byte)(value ? 1 : 0);

        public static void EncodeInt8(Span<byte> destination, sbyte value) =>
            destination[0] = (byte)value;

        public static void EncodeUInt8(Span<byte> destination, byte value) =>
            destination[0] = value;

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

        public static byte[] EncodeUInt8(byte value) =>
            new byte[] { value };

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

        public static byte[] EncodeDefaultValue(ColumnSchema columnSchema, object value)
        {
            var type = columnSchema.Type;

            return type switch
            {
                KuduType.Int8 => EncodeInt8((sbyte)value),
                KuduType.Int16 => EncodeInt16((short)value),
                KuduType.Int32 => EncodeInt32((int)value),
                KuduType.Int64 => EncodeInt64((long)value),
                KuduType.String => EncodeString((string)value),
                KuduType.Varchar => EncodeString((string)value),
                KuduType.Bool => EncodeBool((bool)value),
                KuduType.Float => EncodeFloat((float)value),
                KuduType.Double => EncodeDouble((double)value),
                KuduType.Binary => (byte[])value,
                KuduType.UnixtimeMicros => EncodeDateTime((DateTime)value),
                KuduType.Date => EncodeDate((DateTime)value),
                KuduType.Decimal32 => EncodeDecimal32(
                    (decimal)value,
                    columnSchema.TypeAttributes.Precision.GetValueOrDefault(),
                    columnSchema.TypeAttributes.Scale.GetValueOrDefault()),
                KuduType.Decimal64 => EncodeDecimal64(
                    (decimal)value,
                    columnSchema.TypeAttributes.Precision.GetValueOrDefault(),
                    columnSchema.TypeAttributes.Scale.GetValueOrDefault()),
                KuduType.Decimal128 => EncodeDecimal128(
                    (decimal)value,
                    columnSchema.TypeAttributes.Precision.GetValueOrDefault(),
                    columnSchema.TypeAttributes.Scale.GetValueOrDefault()),
                _ => throw new Exception($"Unknown data type {type}"),
            };
        }

        public static object DecodeDefaultValue(
            KuduType type, ColumnTypeAttributes typeAttributes, ReadOnlySpan<byte> value)
        {
            return type switch
            {
                KuduType.Int8 => DecodeInt8(value),
                KuduType.Int16 => DecodeInt16(value),
                KuduType.Int32 => DecodeInt32(value),
                KuduType.Int64 => DecodeInt64(value),
                KuduType.String => DecodeString(value),
                KuduType.Varchar => DecodeString(value),
                KuduType.Bool => DecodeBool(value),
                KuduType.Float => DecodeFloat(value),
                KuduType.Double => DecodeDouble(value),
                KuduType.Binary => value.ToArray(),
                KuduType.UnixtimeMicros => DecodeDateTime(value),
                KuduType.Date => DecodeDate(value),
                KuduType.Decimal32 => DecodeDecimal(
                    value, type, typeAttributes.Scale.GetValueOrDefault()),
                KuduType.Decimal64 => DecodeDecimal(
                    value, type, typeAttributes.Scale.GetValueOrDefault()),
                KuduType.Decimal128 => DecodeDecimal(
                    value, type, typeAttributes.Scale.GetValueOrDefault()),
                _ => throw new Exception($"Unknown data type {type}"),
            };
        }

        public static bool DecodeBool(ReadOnlySpan<byte> source) => source[0] > 0;

        public static sbyte DecodeInt8(ReadOnlySpan<byte> source) => (sbyte)source[0];

        public static byte DecodeUInt8(ReadOnlySpan<byte> source) => source[0];

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

                default:
                    KuduInt128 int128Val = DecodeInt128(source);
                    return DecimalUtil.DecodeDecimal128(int128Val, scale);
            }
        }

        public static string DecodeString(ReadOnlySpan<byte> source) =>
            Encoding.UTF8.GetString(source);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool DecodeBool(byte[] source, int offset) => source[offset] > 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte DecodeUInt8(byte[] source, int offset) => source[offset];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short DecodeInt16(byte[] source, int offset)
        {
            short result = BitConverter.ToInt16(source, offset);
            if (!BitConverter.IsLittleEndian)
            {
                result = BinaryPrimitives.ReverseEndianness(result);
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int DecodeInt32(byte[] source, int offset)
        {
            int result = BitConverter.ToInt32(source, offset);
            if (!BitConverter.IsLittleEndian)
            {
                result = BinaryPrimitives.ReverseEndianness(result);
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long DecodeInt64(byte[] source, int offset)
        {
            long result = BitConverter.ToInt64(source, offset);
            if (!BitConverter.IsLittleEndian)
            {
                result = BinaryPrimitives.ReverseEndianness(result);
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static KuduInt128 DecodeInt128(byte[] source, int offset)
        {
            var low = (ulong)DecodeInt64(source, offset);
            var high = DecodeInt64(source, offset + 8);

            return new KuduInt128(high, low);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTime DecodeDate(byte[] source, int offset)
        {
            int days = DecodeInt32(source, offset);
            return EpochTime.FromUnixTimeDays(days);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTime DecodeDateTime(byte[] source, int offset)
        {
            long micros = DecodeInt64(source, offset);
            return EpochTime.FromUnixTimeMicros(micros);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float DecodeFloat(byte[] source, int offset)
        {
            int value = DecodeInt32(source, offset);
            return value.AsFloat();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double DecodeDouble(byte[] source, int offset)
        {
            long value = DecodeInt64(source, offset);
            return value.AsDouble();
        }

        public static decimal DecodeDecimal(byte[] source, int offset, KuduType kuduType, int scale)
        {
            switch (kuduType)
            {
                case KuduType.Decimal32:
                    int intVal = DecodeInt32(source, offset);
                    return DecimalUtil.DecodeDecimal32(intVal, scale);

                case KuduType.Decimal64:
                    long longVal = DecodeInt64(source, offset);
                    return DecimalUtil.DecodeDecimal64(longVal, scale);

                default:
                    KuduInt128 int128Val = DecodeInt128(source, offset);
                    return DecimalUtil.DecodeDecimal128(int128Val, scale);
            }
        }

        public static string DecodeString(byte[] source, int offset, int length) =>
            Encoding.UTF8.GetString(source, offset, length);

        public static int BitsToBytes(int bits) => (int)(((uint)bits + 7) / 8);

        /// <summary>
        /// This method will apply xor on the left most bit of the first byte in
        /// the buffer. This is used in Kudu to have unsigned data types sorting
        /// correctly.
        /// </summary>
        /// <param name="buffer">Buffer whose left most bit will be xor'd.</param>
        public static byte XorLeftMostBit(Span<byte> buffer) => buffer[0] ^= 1 << 7;
    }
}
