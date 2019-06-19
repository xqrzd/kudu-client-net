using System;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Kudu.Client.Util
{
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
        public const int Decimal128Size = 16;

        public const int MaxDecimalPrecision = MaxDecimal128Precision;

        private static readonly int[] Pow10Cache32 = {
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

        private static readonly long[] Pow10Cache64 = {
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
            1000000000000000000
        };

        /// <summary>
        /// Given a precision, returns the size of the decimal in bytes.
        /// </summary>
        /// <param name="precision">The precision of the decimal.</param>
        public static int PrecisionToSize(int precision)
        {
            if (precision <= MaxDecimal32Precision)
            {
                return Decimal32Size;
            }
            else if (precision <= MaxDecimal64Precision)
            {
                return Decimal64Size;
            }
            else if (precision <= MaxDecimal128Precision)
            {
                return Decimal128Size;
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    nameof(precision),
                    "Unsupported decimal type precision: " + precision);
            }
        }

        public static int EncodeDecimal32(decimal value, int targetPrecision, int targetScale)
        {
            var dec = new DecimalAccessor(value);
            int scale = dec.Scale;

            CheckConditions(value, scale, targetPrecision, targetScale);

            int scaleAdjustment = targetScale - scale;
            int maxValue = Pow10Int32(targetPrecision - scaleAdjustment) - 1;
            int unscaledValue = dec.Low;

            if (dec.High > 0 || dec.Mid > 0 || unscaledValue > maxValue)
            {
                ThrowValueTooBig(value, targetPrecision);
            }

            int factor = Pow10Int32(scaleAdjustment);
            int result = checked(unscaledValue * factor);

            return dec.IsNegative ? result * -1 : result;
        }

        public static long EncodeDecimal64(decimal value, int targetPrecision, int targetScale)
        {
            var dec = new DecimalAccessor(value);
            int scale = dec.Scale;

            CheckConditions(value, scale, targetPrecision, targetScale);

            int scaleAdjustment = targetScale - scale;
            long maxValue = Pow10Int64(targetPrecision - scaleAdjustment) - 1;
            long unscaledValue = ToLong(dec.Low, dec.Mid);

            if (dec.High > 0 || unscaledValue > maxValue)
            {
                ThrowValueTooBig(value, targetPrecision);
            }

            long factor = Pow10Int64(scaleAdjustment);
            long result = checked(unscaledValue * factor);

            return dec.IsNegative ? result * -1 : result;
        }

        public static BigInteger EncodeDecimal128(decimal value, int targetPrecision, int targetScale)
        {
            var dec = new DecimalAccessor(value);
            int scale = dec.Scale;

            CheckConditions(value, scale, targetPrecision, targetScale);

            var scaleAdjustment = targetScale - dec.Scale;
            var maxValue = BigInteger.Pow(10, targetPrecision - scaleAdjustment) - 1;

            Span<int> data = stackalloc int[3];
            data[0] = dec.Low;
            data[1] = dec.Mid;
            data[2] = dec.High;

            var unscaledValue = new BigInteger(
                MemoryMarshal.Cast<int, byte>(data),
                isUnsigned: true, isBigEndian: false);

            if (unscaledValue > maxValue)
            {
                ThrowValueTooBig(value, targetPrecision);
            }

            var factor = BigInteger.Pow(10, scaleAdjustment);
            var result = unscaledValue * factor;

            return dec.IsNegative ? result * -1 : result;
        }

        private static long ToLong(int lower, int upper)
        {
            long b = (uint)upper;
            b <<= 32;
            b |= (uint)lower;
            return b;
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
                ThrowValueTooBig(value, targetPrecision);
            }
        }

        private static void ThrowValueTooBig(decimal value, int targetPrecision)
        {
            throw new ArgumentException(
                $"Value {value} (after scale coercion) can't be coerced to target precision {targetPrecision}.");
        }

        private static int Pow10Int32(int value) => Pow10Cache32[value];

        private static long Pow10Int64(int value) => Pow10Cache64[value];

        /// <summary>
        /// Provides access to the inner fields of a decimal.
        /// Similar to decimal.GetBits(), but faster and avoids the int[] allocation
        /// </summary>
        [StructLayout(LayoutKind.Explicit)]
        private readonly struct DecimalAccessor
        {
            [FieldOffset(0)]
            public readonly int Flags;
            [FieldOffset(4)]
            public readonly int High;
            [FieldOffset(8)]
            public readonly int Low;
            [FieldOffset(12)]
            public readonly int Mid;

            [FieldOffset(0)]
            public readonly decimal Decimal;

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
            public int Scale => (Flags & 0x00FF0000) >> 16;

            public bool IsNegative => (Flags & 0x80000000) > 0;
        }
    }
}
