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

        public Task<KuduConnection> CreateConnectionAsync(ServerInfo serverInfo)
        {
            HostAndPort hostPort = serverInfo.HostPort;

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

                var task = ConnectAsync(serverInfo);
                _connections[hostPort] = task;
                return task;
            }
        }

        private async Task<KuduConnection> ConnectAsync(ServerInfo serverInfo)
        {
            var connection = await KuduConnectionFactory.ConnectAsync(serverInfo).ConfigureAwait(false);

            // Once we have a KuduConnection, register a callback when it's closed,
            // so we can remove it from the connection cache.

            // TODO: Is it worth trying to avoid a closure here?
            // We'd need to store both _connections and hostPort in state.
            connection.OnConnectionClosed(RemoveConnectionAsync, serverInfo.HostPort);

            return connection;
        }

        private async void RemoveConnectionAsync(Exception ex, object state)
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
                await DisposeConnectionTask(connectionTask).ConfigureAwait(false);
            }
        }

        private async Task DisposeConnectionTask(Task<KuduConnection> connectionTask)
        {
            try
            {
                var connection = await connectionTask.ConfigureAwait(false);
                await connection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception disposing connection {ex}");
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
                await DisposeConnectionTask(connectionTask).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }
    }
}
