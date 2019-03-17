using System.Threading.Tasks;

namespace Kudu.Client.Connection
{
    public interface IKuduConnectionFactory
    {
        Task<KuduConnection> ConnectAsync(ServerInfo serverInfo);
    }
}
