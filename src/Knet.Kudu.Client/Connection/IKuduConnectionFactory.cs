using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.Connection;

public interface IKuduConnectionFactory
{
    Task<KuduConnection> ConnectAsync(
        ServerInfo serverInfo, CancellationToken cancellationToken = default);

    Task<List<ServerInfo>> GetMasterServerInfoAsync(
        HostAndPort hostPort, CancellationToken cancellationToken = default);

    Task<ServerInfo> GetTabletServerInfoAsync(
        HostAndPort hostPort, string uuid, string location, CancellationToken cancellationToken = default);
}
