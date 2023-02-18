using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests;

public class FastHashTests
{
    [Theory]
    [InlineData("ab", 0, 17293172613997361769)]
    [InlineData("abcdefg", 0, 10206404559164245992)]
    [InlineData("quick brown fox", 42, 3757424404558187042)]
    [InlineData("", 0, 4144680785095980158)]
    [InlineData("", 1234, 3296774803014270295)]
    public void TestFastHash64(string data, ulong seed, ulong expectedHash)
    {
        ulong hash = FastHash.Hash64(data.ToUtf8ByteArray(), seed);
        Assert.Equal(expectedHash, hash);
    }

    [Theory]
    [InlineData("ab", 0, 2564147595)]
    [InlineData("abcdefg", 0, 1497700618)]
    [InlineData("quick brown fox", 42, 1676541068)]
    [InlineData("", 0, 3045300040)]
    [InlineData("", 1234, 811548192)]
    public void TestFastHash32(string data, uint seed, uint expectedHash)
    {
        uint hash = FastHash.Hash32(data.ToUtf8ByteArray(), seed);
        Assert.Equal(expectedHash, hash);
    }
}
