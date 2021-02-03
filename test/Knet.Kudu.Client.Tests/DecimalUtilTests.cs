using System;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class DecimalUtilTests
    {
        [Theory]
        [InlineData(6023345402697246, 15782978151453050464, 10)]   // 11111111111111111111111111.11111
        [InlineData(-6023345402697247, 2663765922256501152, 10)]   //-11111111111111111111111111.11111
        [InlineData(667386670618854951, 11070687437558349184, 35)] // 123.1111111111111111111111111111
        [InlineData(-667386670618854952, 7376056636151202432, 35)] //-123.1111111111111111111111111111
        public void KuduDecimalTooLarge(long high, ulong low, int scale)
        {
            var value = new KuduInt128(high, low);

            Assert.Throws<OverflowException>(
                () => DecimalUtil.DecodeDecimal128(value, scale));
        }
    }
}
