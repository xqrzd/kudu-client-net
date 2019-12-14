using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    /// <summary>
    /// Tests from https://github.com/StackExchange/StackExchange.Redis/blob/master/tests/StackExchange.Redis.Tests/FormatTests.cs
    /// </summary>
    public class EndpointParserTests
    {
        [Theory]
        [InlineData("localhost", "localhost", 0)]
        [InlineData("localhost:6390", "localhost", 6390)]
        [InlineData("bob.the.builder.com", "bob.the.builder.com", 0)]
        [InlineData("bob.the.builder.com:6390", "bob.the.builder.com", 6390)]
        // IPv4
        [InlineData("0.0.0.0", "0.0.0.0", 0)]
        [InlineData("127.0.0.1", "127.0.0.1", 0)]
        [InlineData("127.1", "127.1", 0)]
        [InlineData("127.1:6389", "127.1", 6389)]
        [InlineData("127.0.0.1:6389", "127.0.0.1", 6389)]
        [InlineData("127.0.0.1:1", "127.0.0.1", 1)]
        [InlineData("127.0.0.1:2", "127.0.0.1", 2)]
        [InlineData("10.10.9.18:2", "10.10.9.18", 2)]
        // IPv6
        [InlineData("::1", "::1", 0)]
        [InlineData("[::1]:6379", "::1", 6379)]
        [InlineData("[::1]", "::1", 0)]
        [InlineData("[::1]:1000", "::1", 1000)]
        [InlineData("[2001:db7:85a3:8d2:1319:8a2e:370:7348]", "2001:db7:85a3:8d2:1319:8a2e:370:7348", 0)]
        [InlineData("[2001:db7:85a3:8d2:1319:8a2e:370:7348]:1000", "2001:db7:85a3:8d2:1319:8a2e:370:7348", 1000)]
        public void CanParseEndpoint(string endpoint, string expectedHost, int expectedPort)
        {
            HostAndPort hostPort = EndpointParser.TryParse(endpoint);

            Assert.Equal(expectedHost, hostPort.Host);
            Assert.Equal(expectedPort, hostPort.Port);
        }

        [Fact]
        public void CanUseDefaultPort()
        {
            HostAndPort hostPort = EndpointParser.TryParse("127.0.0.1", 7051);

            Assert.Equal("127.0.0.1", hostPort.Host);
            Assert.Equal(7051, hostPort.Port);
        }
    }
}
