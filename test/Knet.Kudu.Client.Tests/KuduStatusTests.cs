using Knet.Kudu.Client.Exceptions;
using Xunit;
using static Knet.Kudu.Client.Protobuf.AppStatusPB.Types;

namespace Knet.Kudu.Client.Tests;

public class KuduStatusTests
{
    [Fact]
    public void TestOkStatus()
    {
        var status = KuduStatus.Ok;
        Assert.True(status.IsOk);
        Assert.False(status.IsNotAuthorized);
        Assert.Equal(-1, status.PosixCode);
        Assert.Equal("Ok", status.ToString());
    }

    [Fact]
    public void TestStatusNonPosix()
    {
        var status = KuduStatus.Aborted("foo");
        Assert.False(status.IsOk);
        Assert.True(status.IsAborted);
        Assert.Equal(ErrorCode.Aborted, status.Code);
        Assert.Equal("foo", status.Message);
        Assert.Equal(-1, status.PosixCode);
        Assert.Equal("Aborted: foo", status.ToString());
    }

    [Fact]
    public void TestPosixCode()
    {
        var status = KuduStatus.NotFound("File not found", 2);
        Assert.False(status.IsOk);
        Assert.False(status.IsAborted);
        Assert.True(status.IsNotFound);
        Assert.Equal(2, status.PosixCode);
        Assert.Equal("Not found: File not found (error 2)", status.ToString());
    }

    [Fact]
    public void TestMessageTooLong()
    {
        int maxMessageLength = KuduStatus.MaxMessageLength;
        string abbreviation = KuduStatus.Abbreviation;
        int abbreviationLength = abbreviation.Length;

        // Test string that will not get abbreviated.
        var str = new string('a', maxMessageLength);
        var status = KuduStatus.Corruption(str);
        Assert.Equal(str, status.Message);

        // Test string just over the limit that will get abbreviated.
        str = new string('a', maxMessageLength + 1);
        status = KuduStatus.Corruption(str);
        Assert.Equal(maxMessageLength, status.Message.Length);
        Assert.Equal(
            status.Message.Substring(maxMessageLength - abbreviationLength),
            abbreviation);

        // Test string that's way too big that will get abbreviated.
        str = new string('a', maxMessageLength * 2);
        status = KuduStatus.Corruption(str);
        Assert.Equal(maxMessageLength, status.Message.Length);
        Assert.Equal(
            status.Message.Substring(maxMessageLength - abbreviationLength),
            abbreviation);
    }
}
