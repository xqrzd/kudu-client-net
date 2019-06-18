using System;
using System.Runtime.InteropServices;

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
            // TODO: Use BitConverter.SingleToInt32Bits() on every platform except .NET Standard 2.0

            // All NaN values are collapsed to a single "canonical" NaN value.
            if (float.IsNaN(value))
                return 0x7fc00000;

            FloatUnion union = value;
            return union.IntValue;
        }

        /// <summary>
        /// Returns the <see cref="float"/> value corresponding to a given bit
        /// representation. The argument is considered to be a representation
        /// of a floating-point value according to the IEEE 754 floating-point
        /// "single format" bit layout.
        /// </summary>
        public static float AsFloat(this int value)
        {
            return BitConverter.Int32BitsToSingle(value);
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

            int bits = BitConverter.SingleToInt32Bits(value + 0.0f);
            return BitConverter.Int32BitsToSingle(bits + ((bits >= 0) ? 1 : -1));
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

        [StructLayout(LayoutKind.Explicit)]
        private struct FloatUnion
        {
            [FieldOffset(0)]
            public float FloatValue;

            [FieldOffset(0)]
            public int IntValue;

            public static implicit operator FloatUnion(int value) =>
                new FloatUnion { IntValue = value };

            public static implicit operator FloatUnion(float value) =>
                new FloatUnion { FloatValue = value };
        }
    }
}
