using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Kudu.Client.Util
{
    [StructLayout(LayoutKind.Explicit)]
    public readonly struct KuduInt128 : IEquatable<KuduInt128>, IComparable<KuduInt128>
    {
        [FieldOffset(0)]
        public readonly long High;
        [FieldOffset(8)]
        public readonly ulong Low;

        public KuduInt128(long high, ulong low)
        {
            Low = low;
            High = high;
        }

        public KuduInt128(long value)
        {
            if (value >= 0)
            {
                High = 0;
                Low = (ulong)value;
            }
            else
            {
                High = -1;
                Low = (ulong)value;
            }
        }

        public KuduInt128(uint part1, uint part2, uint part3, int part4)
        {
            Low = ((ulong)part2 << 32) | part1;
            High = ((long)part4 << 32) | part3;
        }

        public KuduInt128 Negate()
        {
            var low = ~Low + 1;
            var high = ~High;

            if (low == 0)
                high++;

            return new KuduInt128(high, low);
        }

        public KuduInt128 Abs()
        {
            if (High < 0)
                return Negate();

            return this;
        }

        public static bool operator ==(KuduInt128 a, KuduInt128 b) =>
            a.High == b.High && a.Low == b.Low;

        public static bool operator !=(KuduInt128 a, KuduInt128 b) =>
            a.High != b.High || a.Low != b.Low;

        public static bool operator <(KuduInt128 a, KuduInt128 b)
        {
            if (a.High == b.High)
                return a.Low < b.Low;
            else
                return a.High < b.High;
        }

        public static bool operator <=(KuduInt128 a, KuduInt128 b)
        {
            if (a.High == b.High)
                return a.Low <= b.Low;
            else
                return a.High <= b.High;
        }

        public static bool operator >(KuduInt128 a, KuduInt128 b)
        {
            if (a.High == b.High)
                return a.Low > b.Low;
            else
                return a.High > b.High;
        }

        public static bool operator >=(KuduInt128 a, KuduInt128 b)
        {
            if (a.High == b.High)
                return a.Low >= b.Low;
            else
                return a.High >= b.High;
        }

        public static implicit operator KuduInt128(int value) => new KuduInt128(value);

        public static KuduInt128 operator -(KuduInt128 a) => a.Negate();

        public static KuduInt128 operator -(KuduInt128 a, KuduInt128 b)
        {
            var diff = a.Low - b.Low;
            var high = a.High - b.High;

            if (diff > a.Low)
                high--;

            return new KuduInt128(high, diff);
        }

        public static KuduInt128 operator +(KuduInt128 a, KuduInt128 b)
        {
            var sum = a.Low + b.Low;
            var high = a.High + b.High;

            if (sum < a.Low)
                high++;

            return new KuduInt128(high, sum);
        }

        public static KuduInt128 operator *(KuduInt128 a, KuduInt128 b)
        {
            const long intMask = 0xffffffff;
            const long carryBit = intMask + 1;

            // Break the left and right numbers into 32 bit chunks
            // so that we can multiply them without overflow.
            ulong L0 = (ulong)a.High >> 32;
            ulong L1 = (ulong)(a.High) & intMask;
            ulong L2 = a.Low >> 32;
            ulong L3 = a.Low & intMask;
            ulong R0 = (ulong)b.High >> 32;
            ulong R1 = (ulong)b.High & intMask;
            ulong R2 = b.Low >> 32;
            ulong R3 = b.Low & intMask;

            ulong product = L3 * R3;
            ulong lowbits = product & intMask;
            ulong sum = product >> 32;
            product = L2 * R3;
            sum += product;
            long highbits = sum < product ? carryBit : 0;
            product = L3 * R2;
            sum += product;
            if (sum < product)
            {
                highbits += carryBit;
            }
            lowbits += sum << 32;
            highbits += (long)sum >> 32;
            highbits += (long)(L1 * R3 + L2 * R2 + L3 * R1);
            highbits += (long)(L0 * R3 + L1 * R2 + L2 * R1 + L3 * R0) << 32;

            return new KuduInt128(highbits, lowbits);
        }

        public int CompareTo(KuduInt128 other)
        {
            if (this < other) return -1;
            if (this > other) return 1;
            return 0;
        }

        public bool Equals(KuduInt128 other) => this == other;

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode() => HashCode.Combine(High, Low);

        public override string ToString()
        {
            var sb = new StringBuilder(40);
            sb.Append("0x");
            sb.Append(High.ToString("x16"));
            sb.Append(Low.ToString("x16"));
            return sb.ToString();
        }

        public static KuduInt128 PowerOf10(int exponent)
        {
            return Pow10Cache.PowerOf10(exponent);
        }

        private static class Pow10Cache
        {
            private static readonly KuduInt128[] _cache = new KuduInt128[]
            {
                Init(0x0000000000000000, 0x0000000000000001), // 1
                Init(0x0000000000000000, 0x000000000000000a), // 10
                Init(0x0000000000000000, 0x0000000000000064), // 100
                Init(0x0000000000000000, 0x00000000000003e8), // 1000
                Init(0x0000000000000000, 0x0000000000002710), // 10000
                Init(0x0000000000000000, 0x00000000000186a0), // 100000
                Init(0x0000000000000000, 0x00000000000f4240), // 1000000
                Init(0x0000000000000000, 0x0000000000989680), // 10000000
                Init(0x0000000000000000, 0x0000000005f5e100), // 100000000
                Init(0x0000000000000000, 0x000000003b9aca00), // 1000000000
                Init(0x0000000000000000, 0x00000002540be400), // 10000000000
                Init(0x0000000000000000, 0x000000174876e800), // 100000000000
                Init(0x0000000000000000, 0x000000e8d4a51000), // 1000000000000
                Init(0x0000000000000000, 0x000009184e72a000), // 10000000000000
                Init(0x0000000000000000, 0x00005af3107a4000), // 100000000000000
                Init(0x0000000000000000, 0x00038d7ea4c68000), // 1000000000000000
                Init(0x0000000000000000, 0x002386f26fc10000), // 10000000000000000
                Init(0x0000000000000000, 0x016345785d8a0000), // 100000000000000000
                Init(0x0000000000000000, 0x0de0b6b3a7640000), // 1000000000000000000
                Init(0x0000000000000000, 0x8ac7230489e80000), // 10000000000000000000
                Init(0x0000000000000005, 0x6bc75e2d63100000), // 100000000000000000000
                Init(0x0000000000000036, 0x35c9adc5dea00000), // 1000000000000000000000
                Init(0x000000000000021e, 0x19e0c9bab2400000), // 10000000000000000000000
                Init(0x000000000000152d, 0x02c7e14af6800000), // 100000000000000000000000
                Init(0x000000000000d3c2, 0x1bcecceda1000000), // 1000000000000000000000000
                Init(0x0000000000084595, 0x161401484a000000), // 10000000000000000000000000
                Init(0x000000000052b7d2, 0xdcc80cd2e4000000), // 100000000000000000000000000
                Init(0x00000000033b2e3c, 0x9fd0803ce8000000), // 1000000000000000000000000000
                Init(0x00000000204fce5e, 0x3e25026110000000), // 10000000000000000000000000000
                Init(0x00000001431e0fae, 0x6d7217caa0000000), // 100000000000000000000000000000
                Init(0x0000000c9f2c9cd0, 0x4674edea40000000), // 1000000000000000000000000000000
                Init(0x0000007e37be2022, 0xc0914b2680000000), // 10000000000000000000000000000000
                Init(0x000004ee2d6d415b, 0x85acef8100000000), // 100000000000000000000000000000000
                Init(0x0000314dc6448d93, 0x38c15b0a00000000), // 1000000000000000000000000000000000
                Init(0x0001ed09bead87c0, 0x378d8e6400000000), // 10000000000000000000000000000000000
                Init(0x0013426172c74d82, 0x2b878fe800000000), // 100000000000000000000000000000000000
                Init(0x00c097ce7bc90715, 0xb34b9f1000000000), // 1000000000000000000000000000000000000
                Init(0x0785ee10d5da46d9, 0x00f436a000000000), // 10000000000000000000000000000000000000
                Init(0x4b3b4ca85a86c47a, 0x098a224000000000), // 100000000000000000000000000000000000000
            };

            private static KuduInt128 Init(ulong high, ulong low)
            {
                return new KuduInt128((long)high, low);
            }

            public static KuduInt128 PowerOf10(int power)
            {
                return _cache[power];
            }
        }
    }
}
