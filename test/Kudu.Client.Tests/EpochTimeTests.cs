using System;
using Kudu.Client.Util;
using Xunit;

namespace Kudu.Client.Tests
{
    public class EpochTimeTests
    {
        [Theory]
        [InlineData("1969-12-31 19:00:00.0", 0)]
        [InlineData("1969-12-31 19:00:00.123456", 123456)]
        [InlineData("1923-11-30 19:44:36.876544", -1454368523123456)]
        [InlineData("2018-12-25 10:44:30.551", 1545752670551000)]
        public void TestDateTimeConversion(string date, long micros)
        {
            var dateTime = DateTime.Parse(date).ToUniversalTime();

            var toMicros = EpochTime.ToUnixEpochMicros(dateTime);
            Assert.Equal(micros, toMicros);

            var fromMicros = EpochTime.FromUnixEpochMicros(micros);
            Assert.Equal(dateTime, fromMicros);
        }

        [Theory]
        [InlineData("2016-08-19T12:12:12.121", "Central Standard Time", 1471626732121000)]
        public void TestNonZuluDateTimeConversion(string date, string timezone, long micros)
        {
            var dateTime = DateTime.Parse(date);
            var timeZone = TimeZoneInfo.FindSystemTimeZoneById(timezone);
            var utcTime = TimeZoneInfo.ConvertTimeToUtc(dateTime, timeZone);
            var localTime = utcTime.ToLocalTime();

            var toMicros = EpochTime.ToUnixEpochMicros(localTime);
            Assert.Equal(micros, toMicros);

            var fromMicros = EpochTime.FromUnixEpochMicros(micros);
            Assert.Equal(utcTime, fromMicros);
        }
    }
}
