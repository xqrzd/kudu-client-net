using System;

namespace Knet.Kudu.Client.Util
{
    /// <summary>
    /// Provides methods for converting to/from Kudu's unixtime_micros.
    /// </summary>
    public static class EpochTime
    {
        /// <summary>
        /// Represents the number of ticks in 1 microsecond. This field is constant.
        /// </summary>
        private const long TicksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000;

        /// <summary>
        /// The minimum value (0001-01-01) for a Kudu date column.
        /// </summary>
        public const int MinDateValue = -719162;

        /// <summary>
        /// The maximum value (9999-12-31) for a Kudu date column.
        /// </summary>
        public const int MaxDateValue = 2932896;

        /// <summary>
        /// Unix epoch zero-point: January 1, 1970 (midnight UTC/GMT).
        /// </summary>
        public static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Converts the given <see cref="DateTime"/> to microseconds since the
        /// Unix epoch (1970-01-01T00:00:00Z). The value is converted to UTC time.
        /// </summary>
        /// <param name="value">The timestamp to convert to microseconds.</param>
        public static long ToUnixTimeMicros(DateTime value)
        {
            var utcValue = value.ToUniversalTime();
            var epochValue = utcValue - UnixEpoch;
            var micros = epochValue.Ticks / TicksPerMicrosecond;
            return micros;
        }

        /// <summary>
        /// Converts a microsecond offset from the Unix epoch (1970-01-01T00:00:00Z)
        /// to a <see cref="DateTime"/>.
        /// </summary>
        /// <param name="micros">The offset in microseconds since the Unix epoch.</param>
        public static DateTime FromUnixTimeMicros(long micros)
        {
            var ticks = UnixEpoch.Ticks + (micros * TicksPerMicrosecond);
            return new DateTime(ticks, DateTimeKind.Utc);
        }

        /// <summary>
        /// Converts the given <see cref="DateTime"/> to days since the
        /// Unix epoch (1970-01-01T00:00:00Z). The value is converted to UTC time.
        /// </summary>
        /// <param name="value">The timestamp to convert to days.</param>
        public static int ToUnixTimeDays(DateTime value)
        {
            var utcValue = value.ToUniversalTime().Date;
            var epochValue = utcValue - UnixEpoch;
            var days = (int)(epochValue.Ticks / TimeSpan.TicksPerDay);
            CheckDateWithinRange(days);
            return days;
        }

        /// <summary>
        /// Converts a day offset from the Unix epoch (1970-01-01T00:00:00Z)
        /// to a <see cref="DateTime"/>.
        /// </summary>
        /// <param name="days">The offset in days since the Unix epoch.</param>
        public static DateTime FromUnixTimeDays(int days)
        {
            CheckDateWithinRange(days);
            var ticks = UnixEpoch.Ticks + (days * TimeSpan.TicksPerDay);
            return new DateTime(ticks, DateTimeKind.Utc);
        }

        public static void CheckDateWithinRange(int days)
        {
            if (days < MinDateValue || days > MaxDateValue)
            {
                throw new ArgumentException(
                    $"Date value '{days}' is out of range '0001-01-01':'9999-12-31'");
            }
        }
    }
}
