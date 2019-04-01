using System.Threading.Tasks;

namespace Kudu.Client.Connection
{
    public interface IKuduConnectionFactory
    {
        Task<KuduConnection> ConnectAsync(ServerInfo serverInfo);

        Task<ServerInfo> GetServerInfoAsync(string uuid, string location, HostAndPort hostPort);
    }
}
