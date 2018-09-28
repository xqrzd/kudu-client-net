using System.Text;
using Kudu.Client.Util;
using Xunit;

namespace Kudu.Client.Tests
{
    public class MurmurHashTests
    {
        [Fact]
        public void TestMurmur2Hash64()
        {
            ulong hash;

            hash = Murmur2.Hash64(Encoding.UTF8.GetBytes("ab"), 0);
            Assert.Equal(7115271465109541368UL, hash);

            hash = Murmur2.Hash64(Encoding.UTF8.GetBytes("abcdefg"), 0);
            Assert.Equal(2601573339036254301UL, hash);

            hash = Murmur2.Hash64(Encoding.UTF8.GetBytes("quick brown fox"), 42);
            Assert.Equal(3575930248840144026UL, hash);
        }
    }
}
