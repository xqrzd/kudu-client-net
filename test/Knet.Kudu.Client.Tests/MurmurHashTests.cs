using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class MurmurHashTests
    {
        [Theory]
        [InlineData("ab", 0, 7115271465109541368UL)]
        [InlineData("abcdefg", 0, 2601573339036254301UL)]
        [InlineData("quick brown fox", 42, 3575930248840144026UL)]
        public void TestMurmur2Hash64(string data, ulong seed, ulong expectedHash)
        {
            ulong hash = Murmur2.Hash64(data.ToUtf8ByteArray(), seed);
            Assert.Equal(expectedHash, hash);
        }
    }
}
