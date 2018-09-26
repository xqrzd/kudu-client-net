using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kudu.Client.Connection
{
    public class ConnectionCache : IDisposable
    {
        private Dictionary<HostAndPort, Task<KuduConnection>> _connections;

        public ConnectionCache()
        {
            _connections = new Dictionary<HostAndPort, Task<KuduConnection>>();
        }

        public Task<KuduConnection> CreateConnectionAsync(HostAndPort hostPort)
        {
            lock (_connections)
            {
                if (_connections.TryGetValue(hostPort, out var connection))
                    return connection;
                else
                {
                    var task = KuduConnectionFactory.ConnectAsync(hostPort);
                    _connections.Add(hostPort, task);
                    return task;
                }
            }
        }

        public void Dispose()
        {
            foreach (var connection in _connections.Values)
            {
                try
                {
                    connection.GetAwaiter().GetResult().Dispose();
                }
                catch
                {
                    // TODO
                    throw;
                }
            }
        }
    }
}
