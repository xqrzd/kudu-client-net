using System;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests;

public class DecimalUtilTests
{
    [Theory]
    [InlineData(6023345402697246, 15782978151453050464, 10)]   // 11111111111111111111111111.11111
    [InlineData(-6023345402697247, 2663765922256501152, 10)]   //-11111111111111111111111111.11111
    [InlineData(667386670618854951, 11070687437558349184, 35)] // 123.1111111111111111111111111111
    [InlineData(-667386670618854952, 7376056636151202432, 35)] //-123.1111111111111111111111111111
    public void KuduDecimalTooLarge(long high, ulong low, int scale)
    {
        var value = new Int128((ulong)high, low);

        Assert.Throws<OverflowException>(
            () => DecimalUtil.DecodeDecimal128(value, scale));
    }

    [Theory]
    // Decimal values with precision of 9 or less are stored in 4 bytes.
    [InlineData(1, KuduType.Decimal32, 4)]
    [InlineData(9, KuduType.Decimal32, 4)]
    // Decimal values with precision of 10 through 18 are stored in 8 bytes.
    [InlineData(10, KuduType.Decimal64, 8)]
    [InlineData(18, KuduType.Decimal64, 8)]
    // Decimal values with precision of 19 through 38 are stored in 16 bytes.
    [InlineData(19, KuduType.Decimal128, 16)]
    [InlineData(38, KuduType.Decimal128, 16)]
    public void PrecisionToSize(int precision, KuduType expectedType, int expectedSize)
    {
        var type = DecimalUtil.PrecisionToKuduType(precision);
        Assert.Equal(expectedType, type);

        var size = DecimalUtil.PrecisionToSize(precision);
        Assert.Equal(expectedSize, size);
    }

    [Fact]
    public void PrecisionTooLarge()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => DecimalUtil.PrecisionToKuduType(39));
        Assert.Throws<ArgumentOutOfRangeException>(() => DecimalUtil.PrecisionToSize(39));
    }
}
