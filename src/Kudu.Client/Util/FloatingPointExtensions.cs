using System;

namespace Kudu.Client.Util
{
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

#if NETSTANDARD2_0
            return Netstandard2Extensions.SingleToInt32Bits(value);
#else
            return BitConverter.SingleToInt32Bits(value);
#endif
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
            if (float.IsNaN(value) || float.IsInfinity(value))
                return value;

#if NETSTANDARD2_0
            int bits = Netstandard2Extensions.SingleToInt32Bits(value + 0.0f);
            return Netstandard2Extensions.Int32BitsToSingle(bits + ((bits >= 0) ? 1 : -1));
#else
            int bits = BitConverter.SingleToInt32Bits(value + 0.0f);
            return BitConverter.Int32BitsToSingle(bits + ((bits >= 0) ? 1 : -1));
#endif
        }

        /// <summary>
        /// Returns the adjacent floating-point value in the direction of
        /// positive infinity.
        /// </summary>
        /// <param name="value">Starting floating-point value.</param>
        public static double NextUp(this double value)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
                return value;

            long bits = BitConverter.DoubleToInt64Bits(value + 0.0d);
            return BitConverter.Int64BitsToDouble(bits + ((bits >= 0L) ? 1L : -1L));
        }
    }
}
