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
                    return connection;
                else
                {
                    var task = KuduConnectionFactory.ConnectAsync(hostPort);
                    RegisterConnectionClosedCallback(task, hostPort);
                    _connections.Add(hostPort, task);
                    return task;
                }
            }
        }

        private void RegisterConnectionClosedCallback(Task<KuduConnection> task, HostAndPort hostPort)
        {
            // Once we have a KuduConnection, register a callback when it's closed,
            // so we can remove it from the connection cache.

            // TODO: Is it worth trying to avoid a closure here?
            // We'd need to store both _connections and hostPort in state.
            task.ContinueWith(
                (t, s) => t.Result.OnConnectionClosed(RemoveConnection, s),
                state: hostPort);
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

            // TODO: Handle exceptions when disposing the connection.
            connectionTask.Result.Dispose();
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
