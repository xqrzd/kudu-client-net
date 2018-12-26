using System;

namespace Kudu.Client.Util
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
        /// Unix epoch zero-point: January 1, 1970 (midnight UTC/GMT).
        /// </summary>
        public static DateTime UnixEpoch { get; } =
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Converts the given <see cref="DateTime"/> to microseconds since the
        /// Unix epoch (1970-01-01T00:00:00Z). The value is converted to UTC time.
        /// </summary>
        /// <param name="value">The timestamp to convert to microseconds.</param>
        public static long ToUnixEpochMicros(DateTime value)
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
        public static DateTime FromUnixEpochMicros(long micros)
        {
            var ticks = UnixEpoch.Ticks + (micros * TicksPerMicrosecond);
            return new DateTime(ticks, DateTimeKind.Utc);
        }
    }
}
