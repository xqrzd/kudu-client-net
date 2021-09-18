using Knet.Kudu.Client.Connection;
using Xunit;

namespace Knet.Kudu.Client.Tests;

public class HostAndPortTests
{
    [Fact]
    public void SameHostAndPort()
    {
        var hostPort1 = new HostAndPort("localhost", 7051);
        var hostPort2 = new HostAndPort("localhost", 7051);

        Assert.Equal(hostPort1, hostPort2);
        Assert.Equal(hostPort1.GetHashCode(), hostPort2.GetHashCode());
    }

    [Fact]
    public void HostShouldBeCaseInsensitive()
    {
        var hostPort1 = new HostAndPort("localhost", 7051);
        var hostPort2 = new HostAndPort("LOCALHOST", 7051);

        Assert.Equal(hostPort1, hostPort2);
        Assert.Equal(hostPort1.GetHashCode(), hostPort2.GetHashCode());
    }

    [Fact]
    public void DifferentHostSamePort()
    {
        var hostPort1 = new HostAndPort("1.2.3.4", 7051);
        var hostPort2 = new HostAndPort("1.2.3.5", 7051);

        Assert.NotEqual(hostPort1, hostPort2);
    }

    [Fact]
    public void DifferentPortSameHost()
    {
        var hostPort1 = new HostAndPort("127.0.0.1", 7051);
        var hostPort2 = new HostAndPort("127.0.0.1", 7050);

        Assert.NotEqual(hostPort1, hostPort2);
    }
}
