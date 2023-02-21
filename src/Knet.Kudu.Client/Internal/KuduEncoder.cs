using System;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Internal;

internal static class KuduEncoder
{
    public static void EncodeBool(Span<byte> destination, bool value) =>
        destination[0] = (byte)(value ? 1 : 0);

#if NET7_0_OR_GREATER
    public static void EncodeInteger<T>(Span<byte> destination, T value)
        where T : IBinaryInteger<T>
    {
        value.TryWriteLittleEndian(destination, out _);
    }
#else
    public static void EncodeInteger(Span<byte> destination, sbyte value) =>
        destination[0] = (byte)value;

    public static void EncodeInteger(Span<byte> destination, byte value) =>
        destination[0] = value;

    public static void EncodeInteger(Span<byte> destination, short value) =>
        BinaryPrimitives.WriteInt16LittleEndian(destination, value);

    public static void EncodeInteger(Span<byte> destination, int value) =>
        BinaryPrimitives.WriteInt32LittleEndian(destination, value);

    public static void EncodeInteger(Span<byte> destination, long value) =>
        BinaryPrimitives.WriteInt64LittleEndian(destination, value);

    public static void EncodeInteger(Span<byte> destination, Int128 value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination, (ulong)value);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(8), (ulong)(value >> 64));
    }
#endif

    public static void EncodeDateTime(Span<byte> destination, DateTime value)
    {
        long micros = EpochTime.ToUnixTimeMicros(value);
        EncodeInteger(destination, micros);
    }

    public static void EncodeDate(Span<byte> destination, DateTime value)
    {
        int days = EpochTime.ToUnixTimeDays(value);
        EncodeInteger(destination, days);
    }

    public static void EncodeFloat(Span<byte> destination, float value)
    {
        int intValue = value.AsInt();
        EncodeInteger(destination, intValue);
    }

    public static void EncodeDouble(Span<byte> destination, double value)
    {
        long longValue = value.AsLong();
        EncodeInteger(destination, longValue);
    }

    public static void EncodeDecimal32(
        Span<byte> destination, decimal value, int precision, int scale)
    {
        int encodedValue = DecimalUtil.EncodeDecimal32(value, precision, scale);
        EncodeInteger(destination, encodedValue);
    }

    public static void EncodeDecimal64(
        Span<byte> destination, decimal value, int precision, int scale)
    {
        long encodedValue = DecimalUtil.EncodeDecimal64(value, precision, scale);
        EncodeInteger(destination, encodedValue);
    }

    public static void EncodeDecimal128(
        Span<byte> destination, decimal value, int precision, int scale)
    {
        var encodedValue = DecimalUtil.EncodeDecimal128(value, precision, scale);
        EncodeInteger(destination, encodedValue);
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
        EncodeInteger(buffer, value);
        return buffer;
    }

    public static byte[] EncodeInt32(int value)
    {
        var buffer = new byte[4];
        EncodeInteger(buffer, value);
        return buffer;
    }

    public static byte[] EncodeInt64(long value)
    {
        var buffer = new byte[8];
        EncodeInteger(buffer, value);
        return buffer;
    }

    public static byte[] EncodeInt128(Int128 value)
    {
        var buffer = new byte[16];
        EncodeInteger(buffer, value);
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
        var buffer = new byte[4];
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

    public static byte[] EncodeValue(ColumnSchema columnSchema, object value)
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
                columnSchema.TypeAttributes!.Precision.GetValueOrDefault(),
                columnSchema.TypeAttributes.Scale.GetValueOrDefault()),
            KuduType.Decimal64 => EncodeDecimal64(
                (decimal)value,
                columnSchema.TypeAttributes!.Precision.GetValueOrDefault(),
                columnSchema.TypeAttributes.Scale.GetValueOrDefault()),
            KuduType.Decimal128 => EncodeDecimal128(
                (decimal)value,
                columnSchema.TypeAttributes!.Precision.GetValueOrDefault(),
                columnSchema.TypeAttributes.Scale.GetValueOrDefault()),
            _ => throw new Exception($"Unknown data type {type}"),
        };
    }

    public static object DecodeDefaultValue(
        KuduType type, ColumnTypeAttributes? typeAttributes, ReadOnlySpan<byte> value)
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
            KuduType.Decimal32 => DecodeDecimal32(value, typeAttributes!.Scale.GetValueOrDefault()),
            KuduType.Decimal64 => DecodeDecimal64(value, typeAttributes!.Scale.GetValueOrDefault()),
            KuduType.Decimal128 => DecodeDecimal128(value, typeAttributes!.Scale.GetValueOrDefault()),
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

    public static Int128 DecodeInt128(ReadOnlySpan<byte> source)
    {
        var lower = BinaryPrimitives.ReadUInt64LittleEndian(source);
        var upper = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(8));

        return new Int128(upper, lower);
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

    public static decimal DecodeDecimal32(ReadOnlySpan<byte> source, int scale)
    {
        int intVal = DecodeInt32(source);
        return DecimalUtil.DecodeDecimal32(intVal, scale);
    }

    public static decimal DecodeDecimal64(ReadOnlySpan<byte> source, int scale)
    {
        long intVal = DecodeInt64(source);
        return DecimalUtil.DecodeDecimal64(intVal, scale);
    }

    public static decimal DecodeDecimal128(ReadOnlySpan<byte> source, int scale)
    {
        Int128 intVal = DecodeInt128(source);
        return DecimalUtil.DecodeDecimal128(intVal, scale);
    }

    public static string DecodeString(ReadOnlySpan<byte> source) =>
        Encoding.UTF8.GetString(source);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool DecodeBitUnsafe(byte[] source, int startIndex, int index)
    {
        int offset = (int)((uint)startIndex + (uint)index / 8);
        byte value = DecodeUInt8Unsafe(source, offset);
        return (value & 1 << (int)((uint)index % 8)) != 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool DecodeBoolUnsafe(byte[] source, int offset) =>
        DecodeUInt8Unsafe(source, offset) > 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte DecodeUInt8Unsafe(byte[] source, int offset) =>
        ReadUnsafe<byte>(source, offset);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static short DecodeInt16Unsafe(byte[] source, int offset)
    {
        short result = ReadUnsafe<short>(source, offset);
        if (!BitConverter.IsLittleEndian)
        {
            result = BinaryPrimitives.ReverseEndianness(result);
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int DecodeInt32Unsafe(byte[] source, int offset)
    {
        int result = ReadUnsafe<int>(source, offset);
        if (!BitConverter.IsLittleEndian)
        {
            result = BinaryPrimitives.ReverseEndianness(result);
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long DecodeInt64Unsafe(byte[] source, int offset)
    {
        long result = ReadUnsafe<long>(source, offset);
        if (!BitConverter.IsLittleEndian)
        {
            result = BinaryPrimitives.ReverseEndianness(result);
        }
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Int128 DecodeInt128Unsafe(byte[] source, int offset)
    {
        var lower = (ulong)DecodeInt64Unsafe(source, offset);
        var upper = (ulong)DecodeInt64Unsafe(source, offset + 8);

        return new Int128(upper, lower);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DateTime DecodeDateUnsafe(byte[] source, int offset)
    {
        int days = DecodeInt32Unsafe(source, offset);
        return EpochTime.FromUnixTimeDays(days);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DateTime DecodeDateTimeUnsafe(byte[] source, int offset)
    {
        long micros = DecodeInt64Unsafe(source, offset);
        return EpochTime.FromUnixTimeMicros(micros);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float DecodeFloatUnsafe(byte[] source, int offset)
    {
        int value = DecodeInt32Unsafe(source, offset);
        return value.AsFloat();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double DecodeDoubleUnsafe(byte[] source, int offset)
    {
        long value = DecodeInt64Unsafe(source, offset);
        return value.AsDouble();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static decimal DecodeDecimal32Unsafe(byte[] source, int offset, int scale)
    {
        int intVal = DecodeInt32Unsafe(source, offset);
        return DecimalUtil.DecodeDecimal32(intVal, scale);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static decimal DecodeDecimal64Unsafe(byte[] source, int offset, int scale)
    {
        long intVal = DecodeInt64Unsafe(source, offset);
        return DecimalUtil.DecodeDecimal64(intVal, scale);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static decimal DecodeDecimal128Unsafe(byte[] source, int offset, int scale)
    {
        Int128 intVal = DecodeInt128Unsafe(source, offset);
        return DecimalUtil.DecodeDecimal128(intVal, scale);
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void XorLeftMostBit(Span<byte> buffer) => buffer[0] ^= 1 << 7;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadUnsafe<T>(byte[] source, int offset)
    {
#if NET5_0_OR_GREATER
        return Unsafe.ReadUnaligned<T>(ref Unsafe.Add(
            ref MemoryMarshal.GetArrayDataReference(source), offset));
#else
        return Unsafe.ReadUnaligned<T>(ref source[offset]);
#endif
    }
}
