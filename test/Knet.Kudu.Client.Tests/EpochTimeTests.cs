using System;
using System.Runtime.InteropServices;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests
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

            var toMicros = EpochTime.ToUnixTimeMicros(dateTime);
            Assert.Equal(micros, toMicros);

            var fromMicros = EpochTime.FromUnixTimeMicros(micros);
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

            var toMicros = EpochTime.ToUnixTimeMicros(localTime);
            Assert.Equal(micros, toMicros);

            var fromMicros = EpochTime.FromUnixTimeMicros(micros);
            Assert.Equal(utcTime, fromMicros);
        }

        [Theory]
        [InlineData(int.MinValue)]
        [InlineData(int.MaxValue)]
        [InlineData(EpochTime.MinDateValue - 1)]
        [InlineData(EpochTime.MaxDateValue + 1)]
        public void TestDateOutOfRange(int days)
        {
            Assert.Throws<ArgumentException>(
                () => EpochTime.CheckDateWithinRange(days));
        }

        [Theory]
        [InlineData("1/1/0001", EpochTime.MinDateValue)]
        [InlineData("12/31/9999", EpochTime.MaxDateValue)]
        public void TestDateConversion(DateTime date, int days)
        {
            var toDays = EpochTime.ToUnixTimeDays(date);
            Assert.Equal(days, toDays);

            var fromDays = EpochTime.FromUnixTimeDays(days);
            Assert.Equal(date, fromDays);
        }

        // https://github.com/dotnet/corefx/issues/11897
        private string GetTimeZone(string windows, string iana) =>
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? windows : iana;
    }
}
