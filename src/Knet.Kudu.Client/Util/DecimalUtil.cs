using System;
using System.Runtime.InteropServices;

namespace Knet.Kudu.Client.Util
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

        private static readonly uint[] Pow10Cache32 = {
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

        private static readonly ulong[] Pow10Cache64 = {
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
                    $"Unsupported decimal type precision: {precision}");
            }
        }

        public static int EncodeDecimal32(decimal value, int targetPrecision, int targetScale)
        {
            var dec = new DecimalAccessor(value);
            var scale = (int)dec.Scale;

            CheckConditions(value, scale, targetPrecision, targetScale);

            int scaleAdjustment = targetScale - scale;
            uint maxValue = PowerOf10Int32(targetPrecision - scaleAdjustment) - 1;
            uint unscaledValue = dec.Low;

            if (dec.High > 0 || dec.Mid > 0 || unscaledValue > maxValue)
                ThrowValueTooBig(value, targetPrecision);

            uint factor = PowerOf10Int32(scaleAdjustment);
            int result = checked((int)(unscaledValue * factor));

            return dec.IsNegative ? result * -1 : result;
        }

        public static long EncodeDecimal64(decimal value, int targetPrecision, int targetScale)
        {
            var dec = new DecimalAccessor(value);
            var scale = (int)dec.Scale;

            CheckConditions(value, scale, targetPrecision, targetScale);

            int scaleAdjustment = targetScale - scale;
            ulong maxValue = PowerOf10Int64(targetPrecision - scaleAdjustment) - 1;
            ulong unscaledValue = ToLong(dec.Low, dec.Mid);

            if (dec.High > 0 || unscaledValue > maxValue)
                ThrowValueTooBig(value, targetPrecision);

            ulong factor = PowerOf10Int64(scaleAdjustment);
            long result = checked((long)(unscaledValue * factor));

            return dec.IsNegative ? result * -1 : result;
        }

        public static KuduInt128 EncodeDecimal128(decimal value, int targetPrecision, int targetScale)
        {
            var dec = new DecimalAccessor(value);
            var scale = (int)dec.Scale;

            CheckConditions(value, scale, targetPrecision, targetScale);

            int scaleAdjustment = targetScale - scale;
            var maxValue = KuduInt128.PowerOf10(targetPrecision - scaleAdjustment) - 1;
            var unscaledValue = new KuduInt128(dec.Low, dec.Mid, dec.High, 0);

            if (unscaledValue > maxValue)
                ThrowValueTooBig(value, targetPrecision);

            var factor = KuduInt128.PowerOf10(scaleAdjustment);
            var result = unscaledValue * factor;

            return dec.IsNegative ? result.Negate() : result;
        }

        public static decimal DecodeDecimal32(int value, int scale)
        {
            return new decimal(Math.Abs(value), 0, 0, value < 0, (byte)scale);
        }

        public static decimal DecodeDecimal64(long value, int scale)
        {
            ulong abs = (ulong)Math.Abs(value);
            int low = (int)(abs & uint.MaxValue);
            int high = (int)(abs >> 32);

            return new decimal(low, high, 0, value < 0, (byte)scale);
        }

        public static decimal DecodeDecimal128(KuduInt128 value, int scale)
        {
            var abs = value.Abs();

            uint low = (uint)(abs.Low & uint.MaxValue);
            uint mid = (uint)(abs.Low >> 32);

            uint high = (uint)(abs.High & uint.MaxValue);
            uint extraHigh = (uint)(abs.High >> 32);

            if (extraHigh > 0)
            {
                throw new OverflowException("Kudu decimal is too large for .NET decimal." +
                    " Use GetRawFixed to read the raw value.");
            }

            var dec = new DecimalAccessor
            {
                Low = low,
                Mid = mid,
                High = high,
                Scale = (uint)scale,
                IsNegative = value < 0
            };

            return dec.Decimal;
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

        public static KuduInt128 MinDecimal128(int precision) =>
            MaxDecimal128(precision).Negate();

        public static KuduInt128 MaxDecimal128(int precision)
        {
            if (precision > MaxDecimal128Precision)
                throw new ArgumentOutOfRangeException(nameof(precision),
                    $"Max precision for decimal128 is {MaxDecimal128Precision}");

            return KuduInt128.PowerOf10(precision) - 1;
        }

        public static decimal SetScale(decimal value, int scale)
        {
            var dec = new DecimalAccessor(value) { Scale = (uint)scale };
            return dec.Decimal;
        }

        private static ulong ToLong(uint low, uint high)
        {
            return ((ulong)high << 32) | low;
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

        private static uint PowerOf10Int32(int value) => Pow10Cache32[value];

        private static ulong PowerOf10Int64(int value) => Pow10Cache64[value];

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
            private const int ScaleMask = 0x00FF0000;

            // Number of bits scale is shifted by.
            private const int ScaleShift = 16;

            [FieldOffset(0)]
            public uint Flags;
            [FieldOffset(4)]
            public uint High;
            [FieldOffset(8)]
            public uint Low;
            [FieldOffset(12)]
            public uint Mid;

            [FieldOffset(0)]
            public decimal Decimal;

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
                set => Flags = value ? Flags | SignMask : Flags & ~SignMask;
            }
        }
    }
}
