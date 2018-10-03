using System;
using System.Collections.Generic;
using System.Linq;
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
                {
                    // Don't hand out connections we know are faulted, create a new one instead.
                    // The client may still get a faulted connection (if the task hasn't completed yet),
                    // but in that case, the client is responsible for retrying.
                    if (!connection.IsFaulted)
                        return connection;
                }

                var task = ConnectAsync(hostPort);
                _connections[hostPort] = task;
                return task;
            }
        }

        private async Task<KuduConnection> ConnectAsync(HostAndPort hostPort)
        {
            var connection = await KuduConnectionFactory.ConnectAsync(hostPort);

            // Once we have a KuduConnection, register a callback when it's closed,
            // so we can remove it from the connection cache.

            // TODO: Is it worth trying to avoid a closure here?
            // We'd need to store both _connections and hostPort in state.
            connection.OnConnectionClosed(RemoveConnection, hostPort);

            return connection;
        }

        private void RemoveConnection(Exception ex, object state)
        {
            // TODO: Debug log connection removed from cache.

            var hostPort = (HostAndPort)state;
            Task<KuduConnection> connectionTask;

            lock (_connections)
            {
                _connections.Remove(hostPort, out connectionTask);
            }

            // Check if the connection exists in the cache. It may have
            // already been removed if Dispose was called.
            if (connectionTask != null)
            {
                connectionTask.Result.Dispose();
            }
        }

        public async Task DisposeAsync()
        {
            List<Task<KuduConnection>> connections;

            lock (_connections)
            {
                connections = _connections.Values.ToList();
                _connections.Clear();
            }

            foreach (var connectionTask in connections)
            {
                try
                {
                    var connection = await connectionTask;
                    connection.Dispose();
                }
                catch
                {
                    // TODO: Log error
                }
            }
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }
    }
}
