using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.Connection
{
    public interface IKuduConnectionFactory
    {
        Task<KuduConnection> ConnectAsync(ServerInfo serverInfo, CancellationToken cancellationToken = default);

        Task<ServerInfo> GetServerInfoAsync(string uuid, string location, HostAndPort hostPort);
    }
}
