using System;
using System.Runtime.InteropServices;
using Kudu.Client.Util;
using Xunit;

namespace Kudu.Client.Tests
{
    public class EpochTimeTests
    {
        [Theory]
        [InlineData("1970-01-01T00:00:00.0000000Z", 0)]
        [InlineData("1970-01-01T00:00:00.1234560Z", 123456)]
        [InlineData("1923-12-01T00:44:36.8765440Z", -1454368523123456)]
        [InlineData("2018-12-25T15:44:30.5510000Z", 1545752670551000)]
        public void TestDateTimeConversion(string date, long micros)
        {
            var dateTime = DateTime.Parse(date).ToUniversalTime();

            var toMicros = EpochTime.ToUnixEpochMicros(dateTime);
            Assert.Equal(micros, toMicros);

            var fromMicros = EpochTime.FromUnixEpochMicros(micros);
            Assert.Equal(dateTime, fromMicros);
        }

        [Fact]
        public void TestNonZuluDateTimeConversion()
        {
            var dateTime = DateTime.Parse("2016-08-19T12:12:12.1210000");
            var micros = 1471626732121000;
            var timeZone = TimeZoneInfo.FindSystemTimeZoneById(
                GetTimeZone("Central Standard Time", "America/Chicago"));

            var utcTime = TimeZoneInfo.ConvertTimeToUtc(dateTime, timeZone);
            var localTime = utcTime.ToLocalTime();

            var toMicros = EpochTime.ToUnixEpochMicros(localTime);
            Assert.Equal(micros, toMicros);

            var fromMicros = EpochTime.FromUnixEpochMicros(micros);
            Assert.Equal(utcTime, fromMicros);
        }

        private string GetTimeZone(string windows, string linux) =>
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? windows : linux;
    }
}
