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

        public const int MaxDecimal128Precision = 38; // TODO: This is outside C# decimal bounds.
        public const int Decimal128Size = 16;

        public const int MaxDecimalPrecision = MaxDecimal128Precision;

        private static readonly int[] PowBase10Cache32 = {
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

        private static readonly long[] PowBase10Cache64 = {
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
            DecimalParts d = value;

            if (d.Scale > targetScale)
            {
                throw new ArgumentException(
                    $"Value scale {d.Scale} can't be coerced to target scale {targetScale}.");
            }

            var scaleAdjustment = targetScale - (int)d.Scale;
            var factor = PowBase10Cache32[scaleAdjustment];
            var maxValue = PowBase10Cache32[targetPrecision - scaleAdjustment] - 1;
            var unscaledValue = (int)d.Low;

            if (d.High > 0 || d.Mid > 0 || unscaledValue > maxValue)
            {
                throw new ArgumentException(
                    $"Value {value} (after scale coercion) can't be coerced to target precision {targetPrecision}.");
            }

            int result = checked(unscaledValue * factor);

            if (d.IsNegative)
                result *= -1;

            return result;
        }

        public static long EncodeDecimal64(decimal value, int targetPrecision, int targetScale)
        {
            DecimalParts d = value;

            if (d.Scale > targetScale)
            {
                throw new ArgumentException(
                    $"Value scale {d.Scale} can't be coerced to target scale {targetScale}.");
            }

            var scaleAdjustment = targetScale - (int)d.Scale;
            var factor = PowBase10Cache64[scaleAdjustment];
            var maxValue = PowBase10Cache64[targetPrecision - scaleAdjustment] - 1;
            var unscaledValue = ToLong(d.Low, d.Mid);

            if (d.High > 0 || unscaledValue > maxValue)
            {
                throw new ArgumentException(
                    $"Value {value} (after scale coercion) can't be coerced to target precision {targetPrecision}.");
            }

            long result = checked(unscaledValue * factor);

            if (d.IsNegative)
                result *= -1;

            return result;
        }

        public static BigInteger EncodeDecimal128(decimal value, int targetPrecision, int targetScale)
        {
            DecimalParts d = value;

            if (d.Scale > targetScale)
            {
                throw new ArgumentException(
                    $"Value scale {d.Scale} can't be coerced to target scale {targetScale}.");
            }

            var scaleAdjustment = targetScale - (int)d.Scale;
            var factor = BigInteger.Pow(10, scaleAdjustment);
            var maxValue = BigInteger.Pow(10, targetPrecision - scaleAdjustment) - 1;

            Span<uint> data = stackalloc uint[3];
            data[0] = d.Low;
            data[1] = d.Mid;
            data[2] = d.High;

            var unscaledValue = new BigInteger(
                MemoryMarshal.Cast<uint, byte>(data),
                isUnsigned: true, isBigEndian: false);

            if (unscaledValue > maxValue)
            {
                throw new ArgumentException(
                    $"Value {value} (after scale coercion) can't be coerced to target precision {targetPrecision}.");
            }

            var result = unscaledValue * factor;

            if (d.IsNegative)
                result *= -1;

            return result;
        }

        private static long ToLong(uint lower, uint upper)
        {
            long b = upper;
            b <<= 32;
            b |= lower;
            return b;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct DecimalParts
        {
            // Sign mask for the flags field. A value of zero in this bit indicates a
            // positive Decimal value, and a value of one in this bit indicates a
            // negative Decimal value.
            private const int SignMask = unchecked((int)0x80000000);

            // Scale mask for the flags field. This byte in the flags field contains
            // the power of 10 to divide the Decimal value by. The scale byte must
            // contain a value between 0 and 28 inclusive.
            private const int ScaleMask = 0x00FF0000;

            // Number of bits scale is shifted by.
            private const int ScaleShift = 16;

            public uint Flags;
            public uint High;
            public uint Low;
            public uint Mid;

            public uint Scale => (Flags & ScaleMask) >> ScaleShift;

            public bool IsNegative => (Flags & SignMask) > 0;

            public static implicit operator DecimalParts(decimal value) =>
                new DecimalUnion { DecimalValue = value }.Parts;
        }

        [StructLayout(LayoutKind.Explicit)]
        private struct DecimalUnion
        {
            [FieldOffset(0)]
            public decimal DecimalValue;

            [FieldOffset(0)]
            public DecimalParts Parts;

            public static implicit operator DecimalUnion(DecimalParts value) =>
                new DecimalUnion { Parts = value };

            public static implicit operator DecimalUnion(decimal value) =>
                new DecimalUnion { DecimalValue = value };
        }
    }
}
