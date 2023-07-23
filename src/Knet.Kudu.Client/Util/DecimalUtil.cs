using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client.Util;

public static class DecimalUtil
{
    public const int MaxDecimal32Precision = 9;
    public const int MaxUnscaledDecimal32 = 999999999;
    public const int MinUnscaledDecimal32 = -MaxUnscaledDecimal32;
    public const int Decimal32Size = 4;

    public const int MaxDecimal64Precision = 18;
    public const long MaxUnscaledDecimal64 = 999999999999999999;
    public const long MinUnscaledDecimal64 = -MaxUnscaledDecimal64;
    public const int Decimal64Size = 8;

    public const int MaxDecimal128Precision = 38;
    public static readonly Int128 MaxUnscaledDecimal128 = new(
        // 99999999999999999999999999999999999999
        0x4b3b4ca85a86c47a, 0x98a223fffffffff);
    public static readonly Int128 MinUnscaledDecimal128 = -MaxUnscaledDecimal128;
    public const int Decimal128Size = 16;

    public const int MaxDecimalPrecision = MaxDecimal128Precision;

    private static readonly uint[] _pow10Cache32 = {
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000
    };

    private static readonly ulong[] _pow10Cache64 = {
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000,
        100000000000,
        1000000000000,
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000
    };

    private static readonly UInt128[] _pow10Cache128 = {
        new UInt128(0x0000000000000000, 0x0000000000000001), // 1
        new UInt128(0x0000000000000000, 0x000000000000000a), // 10
        new UInt128(0x0000000000000000, 0x0000000000000064), // 100
        new UInt128(0x0000000000000000, 0x00000000000003e8), // 1000
        new UInt128(0x0000000000000000, 0x0000000000002710), // 10000
        new UInt128(0x0000000000000000, 0x00000000000186a0), // 100000
        new UInt128(0x0000000000000000, 0x00000000000f4240), // 1000000
        new UInt128(0x0000000000000000, 0x0000000000989680), // 10000000
        new UInt128(0x0000000000000000, 0x0000000005f5e100), // 100000000
        new UInt128(0x0000000000000000, 0x000000003b9aca00), // 1000000000
        new UInt128(0x0000000000000000, 0x00000002540be400), // 10000000000
        new UInt128(0x0000000000000000, 0x000000174876e800), // 100000000000
        new UInt128(0x0000000000000000, 0x000000e8d4a51000), // 1000000000000
        new UInt128(0x0000000000000000, 0x000009184e72a000), // 10000000000000
        new UInt128(0x0000000000000000, 0x00005af3107a4000), // 100000000000000
        new UInt128(0x0000000000000000, 0x00038d7ea4c68000), // 1000000000000000
        new UInt128(0x0000000000000000, 0x002386f26fc10000), // 10000000000000000
        new UInt128(0x0000000000000000, 0x016345785d8a0000), // 100000000000000000
        new UInt128(0x0000000000000000, 0x0de0b6b3a7640000), // 1000000000000000000
        new UInt128(0x0000000000000000, 0x8ac7230489e80000), // 10000000000000000000
        new UInt128(0x0000000000000005, 0x6bc75e2d63100000), // 100000000000000000000
        new UInt128(0x0000000000000036, 0x35c9adc5dea00000), // 1000000000000000000000
        new UInt128(0x000000000000021e, 0x19e0c9bab2400000), // 10000000000000000000000
        new UInt128(0x000000000000152d, 0x02c7e14af6800000), // 100000000000000000000000
        new UInt128(0x000000000000d3c2, 0x1bcecceda1000000), // 1000000000000000000000000
        new UInt128(0x0000000000084595, 0x161401484a000000), // 10000000000000000000000000
        new UInt128(0x000000000052b7d2, 0xdcc80cd2e4000000), // 100000000000000000000000000
        new UInt128(0x00000000033b2e3c, 0x9fd0803ce8000000), // 1000000000000000000000000000
        new UInt128(0x00000000204fce5e, 0x3e25026110000000), // 10000000000000000000000000000
        new UInt128(0x00000001431e0fae, 0x6d7217caa0000000), // 100000000000000000000000000000
        new UInt128(0x0000000c9f2c9cd0, 0x4674edea40000000), // 1000000000000000000000000000000
        new UInt128(0x0000007e37be2022, 0xc0914b2680000000), // 10000000000000000000000000000000
        new UInt128(0x000004ee2d6d415b, 0x85acef8100000000), // 100000000000000000000000000000000
        new UInt128(0x0000314dc6448d93, 0x38c15b0a00000000), // 1000000000000000000000000000000000
        new UInt128(0x0001ed09bead87c0, 0x378d8e6400000000), // 10000000000000000000000000000000000
        new UInt128(0x0013426172c74d82, 0x2b878fe800000000), // 100000000000000000000000000000000000
        new UInt128(0x00c097ce7bc90715, 0xb34b9f1000000000), // 1000000000000000000000000000000000000
        new UInt128(0x0785ee10d5da46d9, 0x00f436a000000000), // 10000000000000000000000000000000000000
        new UInt128(0x4b3b4ca85a86c47a, 0x098a224000000000), // 100000000000000000000000000000000000000
    };

    /// <summary>
    /// Given a precision, returns the size of the decimal in bytes.
    /// </summary>
    /// <param name="precision">The precision of the decimal.</param>
    public static int PrecisionToSize(int precision)
    {
        return precision switch
        {
            <= MaxDecimal32Precision => Decimal32Size,
            <= MaxDecimal64Precision => Decimal64Size,
            <= MaxDecimal128Precision => Decimal128Size,
            _ => throw new ArgumentOutOfRangeException(
                nameof(precision),
                $"Unsupported decimal type precision: {precision}")
        };
    }

    /// <summary>
    /// Given a precision, returns the smallest unscaled data type.
    /// </summary>
    /// <param name="precision">The precision of the decimal.</param>
    public static KuduType PrecisionToKuduType(int precision)
    {
        return precision switch
        {
            <= MaxDecimal32Precision => KuduType.Decimal32,
            <= MaxDecimal64Precision => KuduType.Decimal64,
            <= MaxDecimal128Precision => KuduType.Decimal128,
            _ => throw new ArgumentOutOfRangeException(
                nameof(precision),
                $"Unsupported decimal type precision: {precision}")
        };
    }

    public static int EncodeDecimal32(decimal value, int targetPrecision, int targetScale)
    {
        var dec = new DecimalAccessor(value);
        int scale = (int)dec.Scale;

        CheckConditions(value, scale, targetPrecision, targetScale);

        int scaleAdjustment = targetScale - scale;
        uint maxValue = PowerOf10Int32(targetPrecision - scaleAdjustment) - 1;
        uint unscaledValue = dec.Low32;

        if (dec.High32 > 0 || dec.Mid > 0 || unscaledValue > maxValue)
            ThrowValueTooBig(value, targetPrecision, targetScale);

        uint factor = PowerOf10Int32(scaleAdjustment);
        int result = (int)(unscaledValue * factor);

        return dec.IsNegative ? result * -1 : result;
    }

    public static long EncodeDecimal64(decimal value, int targetPrecision, int targetScale)
    {
        var dec = new DecimalAccessor(value);
        int scale = (int)dec.Scale;

        CheckConditions(value, scale, targetPrecision, targetScale);

        int scaleAdjustment = targetScale - scale;
        ulong maxValue = PowerOf10Int64(targetPrecision - scaleAdjustment) - 1;
        ulong unscaledValue = dec.Low64;

        if (dec.High32 > 0 || unscaledValue > maxValue)
            ThrowValueTooBig(value, targetPrecision, targetScale);

        ulong factor = PowerOf10Int64(scaleAdjustment);
        long result = (long)(unscaledValue * factor);

        return dec.IsNegative ? result * -1 : result;
    }

    public static Int128 EncodeDecimal128(decimal value, int targetPrecision, int targetScale)
    {
        var dec = new DecimalAccessor(value);
        int scale = (int)dec.Scale;

        CheckConditions(value, scale, targetPrecision, targetScale);

        int scaleAdjustment = targetScale - scale;
        var maxValue = PowerOf10Int128(targetPrecision - scaleAdjustment) - 1;
        var unscaledValue = new UInt128(dec.High32, dec.Low64);

        if (unscaledValue > maxValue)
            ThrowValueTooBig(value, targetPrecision, targetScale);

        var factor = PowerOf10Int128(scaleAdjustment);
        var result = (Int128)(unscaledValue * factor);

        return dec.IsNegative ? result * -1 : result;
    }

    public static decimal DecodeDecimal32(int value, int scale)
    {
        return new decimal(Math.Abs(value), 0, 0, value < 0, (byte)scale);
    }

    public static decimal DecodeDecimal64(long value, int scale)
    {
        ulong abs = (ulong)Math.Abs(value);
        int low = (int)abs;
        int high = (int)(abs >> 32);

        return new decimal(low, high, 0, value < 0, (byte)scale);
    }

    public static decimal DecodeDecimal128(Int128 value, int scale)
    {
        var abs = (UInt128)Int128.Abs(value);

        var low64 = (ulong)abs;
        var high64 = (ulong)(abs >> 64);

        int low = (int)low64;
        int mid = (int)(low64 >> 32);
        int high = (int)high64;

        // .NET decimal only has 96 bits, throw an exception if the upper 32 bits are used.
        if ((high64 >> 32) > 0)
        {
            throw new OverflowException("Kudu decimal is too large for .NET decimal. " +
                $"Use {nameof(RowResult.GetSpan)} to read the raw value.");
        }

        return new decimal(low, mid, high, value < 0, (byte)scale);
    }

    public static int MinDecimal32(int precision) =>
        MaxDecimal32(precision) * -1;

    public static int MaxDecimal32(int precision)
    {
        if (precision > MaxDecimal32Precision)
            throw new ArgumentOutOfRangeException(nameof(precision),
                $"Max precision for decimal32 is {MaxDecimal32Precision}");

        return (int)PowerOf10Int32(precision) - 1;
    }

    public static long MinDecimal64(int precision) =>
        MaxDecimal64(precision) * -1;

    public static long MaxDecimal64(int precision)
    {
        if (precision > MaxDecimal64Precision)
            throw new ArgumentOutOfRangeException(nameof(precision),
                $"Max precision for decimal64 is {MaxDecimal64Precision}");

        return (long)PowerOf10Int64(precision) - 1;
    }

    public static Int128 MinDecimal128(int precision) =>
        MaxDecimal128(precision) * -1;

    public static Int128 MaxDecimal128(int precision)
    {
        if (precision > MaxDecimal128Precision)
            throw new ArgumentOutOfRangeException(nameof(precision),
                $"Max precision for decimal128 is {MaxDecimal128Precision}");

        return (Int128)PowerOf10Int128(precision) - 1;
    }

    internal static decimal SetScale(decimal value, int scale)
    {
        var dec = new DecimalAccessor(value) { Scale = (uint)scale };
        return dec.Decimal;
    }

    private static void CheckConditions(decimal value, int scale, int targetPrecision, int targetScale)
    {
        if (scale > targetScale)
        {
            throw new ArgumentException(
                $"Value scale {scale} can't be coerced to target scale {targetScale}.");
        }

        if (targetScale > targetPrecision)
        {
            ThrowValueTooBig(value, targetPrecision, targetScale);
        }
    }

    [DoesNotReturn]
    private static void ThrowValueTooBig(decimal value, int targetPrecision, int targetScale)
    {
        throw new ArgumentException(
            $"Value {value} (after scale coercion of {targetScale}) can't be coerced " +
            $"to target precision {targetPrecision}.");
    }

    private static uint PowerOf10Int32(int value) => _pow10Cache32[value];

    private static ulong PowerOf10Int64(int value) => _pow10Cache64[value];

    private static UInt128 PowerOf10Int128(int value) => _pow10Cache128[value];

    /// <summary>
    /// Provides access to the inner fields of a decimal.
    /// Similar to decimal.GetBits(), but faster and avoids the int[] allocation
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    private struct DecimalAccessor
    {
        // Sign mask for the flags field. A value of zero in this bit indicates a
        // positive Decimal value, and a value of one in this bit indicates a
        // negative Decimal value.
        private const uint SignMask = 0x80000000;

        // Scale mask for the flags field. This byte in the flags field contains
        // the power of 10 to divide the Decimal value by. The scale byte must
        // contain a value between 0 and 28 inclusive.
        private const int ScaleMask = 0x00ff0000;

        // Number of bits scale is shifted by.
        private const int ScaleShift = 16;

        [FieldOffset(0)]
        public uint Flags;
        [FieldOffset(4)]
        public uint High32;
        [FieldOffset(8)]
        public ulong Low64;

        [FieldOffset(0)]
        public decimal Decimal;

        public readonly uint Low32 => (uint)Low64;

        public readonly uint Mid => (uint)(Low64 >> 32);

        public DecimalAccessor(decimal value)
        {
            this = default;
            Decimal = value;
        }

        /// <summary>
        /// This byte in the flags field contains the power
        /// of 10 to divide the Decimal value by. The scale
        /// byte must contain a value between 0 and 28 inclusive.
        /// </summary>
        public uint Scale
        {
            get => (Flags & ScaleMask) >> ScaleShift;
            set => Flags = ((value << ScaleShift) & ScaleMask) | (Flags & SignMask);
        }

        public bool IsNegative
        {
            get => (Flags & SignMask) > 0;
            set => Flags = value ? (Flags | SignMask) : (Flags & ~SignMask);
        }
    }
}
