using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class FastHashTests
    {
        [Theory]
        [InlineData("ab", 0, 17293172613997361769UL)]
        [InlineData("abcdefg", 0, 10206404559164245992UL)]
        [InlineData("quick brown fox", 42, 3757424404558187042UL)]
        public void TestFastHash64(string data, ulong seed, ulong expectedHash)
        {
            ulong hash = FastHash.Hash64(data.ToUtf8ByteArray(), seed);
            Assert.Equal(expectedHash, hash);
        }

        [Theory]
        [InlineData("ab", 0, 2564147595U)]
        [InlineData("abcdefg", 0, 1497700618U)]
        [InlineData("quick brown fox", 42, 1676541068U)]
        public void TestFastHash32(string data, uint seed, uint expectedHash)
        {
            uint hash = FastHash.Hash32(data.ToUtf8ByteArray(), seed);
            Assert.Equal(expectedHash, hash);
        }
    }
}
