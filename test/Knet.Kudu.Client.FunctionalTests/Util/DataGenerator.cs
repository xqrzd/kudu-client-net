using System;
using System.Numerics;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.FunctionalTests.Util
{
    /// <summary>
    /// A utility class to generate random data and rows.
    /// </summary>
    public class DataGenerator
    {
        private readonly Random _random;
        private readonly int _stringLength;
        private readonly int _binaryLength;
        private readonly double _nullRate;
        private readonly double _defaultRate;

        public DataGenerator(
            Random random,
            int stringLength,
            int binaryLength,
            double nullRate,
            double defaultRate)
        {
            _random = random;
            _stringLength = stringLength;
            _binaryLength = binaryLength;
            _nullRate = nullRate;
            _defaultRate = defaultRate;
        }

        /// <summary>
        /// Randomizes the fields in a given PartialRow.
        /// </summary>
        /// <param name="row">The PartialRow to randomize.</param>
        public void RandomizeRow(PartialRow row)
        {
            RandomizeRow(row, true);
        }

        /// <summary>
        /// Randomizes the fields in a given PartialRow.
        /// </summary>
        /// <param name="row">The PartialRow to randomize.</param>
        /// <param name="randomizeKeys">True if the key columns should be randomized.</param>
        public void RandomizeRow(PartialRow row, bool randomizeKeys)
        {
            var schema = row.Schema;
            var columns = schema.Columns;
            foreach (var col in columns)
            {
                if (col.IsKey && !randomizeKeys)
                    continue;

                var type = col.Type;
                var name = col.Name;

                if (col.IsNullable && _random.NextDouble() <= _nullRate)
                {
                    // Sometimes set nullable columns to null.
                    row.SetNull(name);
                    continue;
                }

                if (col.DefaultValue != null && !col.IsKey && _random.NextDouble() <= _defaultRate)
                {
                    // Sometimes use the column default value.
                    continue;
                }

                switch (type)
                {
                    case KuduType.Bool:
                        row.SetBool(name, _random.NextBool());
                        break;
                    case KuduType.Int8:
                        row.SetByte(name, (byte)_random.Next());
                        break;
                    case KuduType.Int16:
                        row.SetInt16(name, (short)_random.Next());
                        break;
                    case KuduType.Int32:
                        row.SetInt32(name, _random.Next());
                        break;
                    case KuduType.Date:
                        row.SetDateTime(name, RandomDate(_random));
                        break;
                    case KuduType.Int64:
                    case KuduType.UnixtimeMicros:
                        row.SetInt64(name, _random.NextLong());
                        break;
                    case KuduType.Float:
                        row.SetFloat(name, (float)_random.NextDouble());
                        break;
                    case KuduType.Double:
                        row.SetDouble(name, _random.NextDouble());
                        break;
                    case KuduType.Decimal32:
                    case KuduType.Decimal64:
                    case KuduType.Decimal128:
                        row.SetDecimal(name, RandomDecimal(col.TypeAttributes, _random));
                        break;
                    case KuduType.String:
                        row.SetString(name, RandomString(_stringLength, _random));
                        break;
                    case KuduType.Varchar:
                        row.SetString(name, RandomString(col.TypeAttributes.Length.Value, _random));
                        break;
                    case KuduType.Binary:
                        row.SetBinary(name, RandomBinary(_binaryLength, _random));
                        break;
                    default:
                        throw new NotSupportedException($"Unsupported type {type}");
                }
            }
        }

        /// <summary>
        /// Utility method to return a random decimal value.
        /// </summary>
        public static decimal RandomDecimal(ColumnTypeAttributes attributes, Random random)
        {
            int numBits = NumBits(attributes.Precision.GetValueOrDefault());
            var randomUnscaled = NextBigInteger(numBits, random);
            return DecimalUtil.SetScale((decimal)randomUnscaled, attributes.Scale.GetValueOrDefault());
        }

        /// <summary>
        /// Utility method to return a random date value.
        /// </summary>
        public static DateTime RandomDate(Random random)
        {
            int bound = EpochTime.MaxDateValue - EpochTime.MinDateValue + 1;
            int days = random.Next(bound) + EpochTime.MinDateValue;
            return EpochTime.FromUnixTimeDays(days);
        }

        /// <summary>
        /// Utility method to return a random string value.
        /// </summary>
        public static string RandomString(int length, Random random)
        {
            var bytes = new byte[length];
            random.NextBytes(bytes);
            return Convert.ToBase64String(bytes);
        }

        /// <summary>
        /// Utility method to return a random binary value.
        /// </summary>
        public static byte[] RandomBinary(int length, Random random)
        {
            var bytes = new byte[length];
            random.NextBytes(bytes);
            return bytes;
        }

        /// <summary>
        /// Generates a random positive BigInteger between 0 and 2^bitLength
        /// (non-inclusive).
        /// https://gist.github.com/rharkanson/50fe61655e80488fcfec7d2ee8eff568
        /// </summary>
        public static BigInteger NextBigInteger(int bitLength, Random random)
        {
            if (bitLength < 1)
                return BigInteger.Zero;

            int bytes = bitLength / 8;
            int bits = bitLength % 8;

            // Generates enough random bytes to cover our bits.
            var bs = new byte[bytes + 1];
            random.NextBytes(bs);

            // Mask out the unnecessary bits.
            byte mask = (byte)(0xFF >> (8 - bits));
            bs[^1] &= mask;

            return new BigInteger(bs);
        }

        private static int NumBits(int precision)
        {
            // int numBits = BigInteger.TEN.pow(attributes.getPrecision())
            // .subtract(BigInteger.ONE).bitCount();
            return precision switch
            {
                0 => 0,
                1 => 2,
                2 => 4,
                3 => 8,
                4 => 8,
                5 => 10,
                6 => 12,
                7 => 14,
                8 => 19,
                9 => 21,
                10 => 20,
                11 => 25,
                12 => 24,
                13 => 26,
                14 => 30,
                15 => 34,
                16 => 35,
                17 => 36,
                18 => 41,
                19 => 37,
                20 => 45,
                21 => 49,
                22 => 46,
                23 => 49,
                24 => 53,
                25 => 43,
                26 => 56,
                27 => 59,
                28 => 56,
                29 => 64,
                30 => 66,
                31 => 63,
                32 => 70,
                33 => 66,
                34 => 75,
                35 => 74,
                36 => 79,
                37 => 78,
                38 => 75,
                _ => throw new ArgumentOutOfRangeException()
            };
        }
    }

    /// <summary>
    /// A builder to configure and construct a DataGenerator instance.
    /// </summary>
    public class DataGeneratorBuilder
    {
        private Random _random = new Random();
        private int _stringLength = 128;
        private int _binaryLength = 128;
        private double _nullRate = 0.1f;
        private double _defaultRate = 0.1f;

        /// <summary>
        /// Define a custom Random instance to use for any random generation.
        /// </summary>
        public DataGeneratorBuilder Random(Random random)
        {
            _random = random;
            return this;
        }

        /// <summary>
        /// Define the length of the data when randomly generating column values for
        /// string columns.
        /// </summary>
        public DataGeneratorBuilder StringLength(int stringLength)
        {
            _stringLength = stringLength;
            return this;
        }

        /// <summary>
        /// Define the length of the data when randomly generating column values for
        /// binary columns.
        /// </summary>
        public DataGeneratorBuilder BinaryLength(int binaryLength)
        {
            _binaryLength = binaryLength;
            return this;
        }

        /// <summary>
        /// Define the rate at which null values should be used when randomly generating
        /// column values.
        /// </summary>
        public DataGeneratorBuilder NullRate(double nullRate)
        {
            if (!(nullRate >= 0f && nullRate <= 1f))
                throw new ArgumentOutOfRangeException(
                    nameof(nullRate), "nullRate must be between 0 and 1");

            _nullRate = nullRate;
            return this;
        }

        /// <summary>
        /// Define the rate at which default values should be used when randomly generating
        /// column values.
        /// </summary>
        public DataGeneratorBuilder DefaultRate(double defaultRate)
        {
            if (!(defaultRate >= 0f && defaultRate <= 1f))
                throw new ArgumentOutOfRangeException(
                    nameof(defaultRate), "defaultRate must be between 0 and 1");

            _defaultRate = defaultRate;
            return this;
        }

        public DataGenerator Build()
        {
            return new DataGenerator(
                _random,
                _stringLength,
                _binaryLength,
                _nullRate,
                _defaultRate);
        }
    }
}
