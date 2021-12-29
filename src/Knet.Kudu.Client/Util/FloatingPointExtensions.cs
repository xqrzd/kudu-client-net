using System;
using System.Runtime.CompilerServices;
using Knet.Kudu.Client.Internal;

namespace Knet.Kudu.Client.Util;

public static class FloatingPointExtensions
{
    /// <summary>
    /// Returns a representation of the specified floating-point value
    /// according to the IEEE 754 floating-point "single format" bit layout.
    /// If the argument is NaN, the result is 0x7fc00000.
    /// </summary>
    /// <param name="value">A floating-point number.</param>
    public static int AsInt(this float value)
    {
        // All NaN values are collapsed to a single "canonical" NaN value.
        if (float.IsNaN(value))
            return 0x7fc00000;

        return SingleToInt32Bits(value);
    }

    /// <summary>
    /// Returns the <see cref="float"/> value corresponding to a given bit
    /// representation. The argument is considered to be a representation
    /// of a floating-point value according to the IEEE 754 floating-point
    /// "single format" bit layout.
    /// </summary>
    public static float AsFloat(this int value)
    {
#if NETSTANDARD2_0
        return Netstandard2Extensions.Int32BitsToSingle(value);
#else
        return BitConverter.Int32BitsToSingle(value);
#endif
    }

    /// <summary>
    /// Returns a representation of the specified floating-point value
    /// according to the IEEE 754 floating-point "double format" bit layout.
    /// If the argument is NaN, the result is 0x7ff8000000000000.
    /// </summary>
    /// <param name="value">A double precision floating-point number.</param>
    public static long AsLong(this double value)
    {
        // All NaN values are collapsed to a single "canonical" NaN value.
        if (double.IsNaN(value))
            return 0x7ff8000000000000;

        return BitConverter.DoubleToInt64Bits(value);
    }

    /// <summary>
    /// Returns the <see cref="double"/> value corresponding to a given bit
    /// representation. The argument is considered to be a representation
    /// of a floating-point value according to the IEEE 754 floating-point
    /// "double format" bit layout.
    /// </summary>
    public static double AsDouble(this long value)
    {
        return BitConverter.Int64BitsToDouble(value);
    }

    /// <summary>
    /// Returns the adjacent floating-point value in the direction of
    /// positive infinity.
    /// </summary>
    /// <param name="value">Starting floating-point value.</param>
    public static float NextUp(this float value)
    {
#if NETCOREAPP3_1_OR_GREATER
        return MathF.BitIncrement(value);
#else
        int bits = SingleToInt32Bits(value);

        if ((bits & 0x7F800000) >= 0x7F800000)
        {
            // NaN returns NaN
            // -Infinity returns float.MinValue
            // +Infinity returns +Infinity
            return (bits == unchecked((int)0xFF800000)) ? float.MinValue : value;
        }

        if (bits == unchecked((int)0x80000000))
        {
            // -0.0 returns float.Epsilon
            return float.Epsilon;
        }

        // Negative values need to be decremented
        // Positive values need to be incremented

        bits += (bits < 0) ? -1 : +1;
        return bits.AsFloat();
#endif
    }

    /// <summary>
    /// Returns the adjacent floating-point value in the direction of
    /// positive infinity.
    /// </summary>
    /// <param name="value">Starting floating-point value.</param>
    public static double NextUp(this double value)
    {
#if NETCOREAPP3_1_OR_GREATER
        return Math.BitIncrement(value);
#else
        long bits = BitConverter.DoubleToInt64Bits(value);

        if (((bits >> 32) & 0x7FF00000) >= 0x7FF00000)
        {
            // NaN returns NaN
            // -Infinity returns double.MinValue
            // +Infinity returns +Infinity
            return (bits == unchecked((long)0xFFF00000_00000000)) ? double.MinValue : value;
        }

        if (bits == unchecked((long)0x80000000_00000000))
        {
            // -0.0 returns double.Epsilon
            return double.Epsilon;
        }

        // Negative values need to be decremented
        // Positive values need to be incremented

        bits += ((bits < 0) ? -1 : +1);
        return BitConverter.Int64BitsToDouble(bits);
#endif
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SingleToInt32Bits(float value)
    {
#if NETSTANDARD2_0
        return Netstandard2Extensions.SingleToInt32Bits(value);
#else
        return BitConverter.SingleToInt32Bits(value);
#endif
    }
}
