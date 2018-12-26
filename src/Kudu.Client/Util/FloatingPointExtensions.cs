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
            // All NaN values are collapsed to a single "canonical" NaN value.
            if (float.IsNaN(value))
                return 0x7fc00000;

            FloatUnion union = value;
            return union.IntValue;
        }

        /// <summary>
        /// Returns a representation of the specified floating-point value
        /// according to the IEEE 754 floating-point "double format" bit layout.
        /// If the argument is NaN, the result is 0x7fc00000.
        /// </summary>
        /// <param name="value">A double precision floating-point number.</param>
        public static long AsLong(this double value)
        {
            if (double.IsNaN(value))
                return 0x7ff8000000000000;

            DoubleUnion union = value;
            return union.LongValue;
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

        [StructLayout(LayoutKind.Explicit)]
        private struct DoubleUnion
        {
            [FieldOffset(0)]
            public double DoubleValue;

            [FieldOffset(0)]
            public long LongValue;

            public static implicit operator DoubleUnion(long value) =>
                new DoubleUnion { LongValue = value };

            public static implicit operator DoubleUnion(double value) =>
                new DoubleUnion { DoubleValue = value };
        }
    }
}
