using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Kudu.Client.Connection
{
    public class ConnectionCache : IDisposable
    {
        private readonly IKuduConnectionFactory _connectionFactory;
        private readonly Dictionary<HostAndPort, Task<KuduConnection>> _connections;
        private bool _disposed;

        public ConnectionCache(IKuduConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
            _connections = new Dictionary<HostAndPort, Task<KuduConnection>>();
        }

        public Task<KuduConnection> GetConnectionAsync(ServerInfo serverInfo)
        {
            var hostPort = serverInfo.HostPort;

            lock (_connections)
            {
                if (_disposed)
                    ThrowDisposedException();

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
            var connection = await _connectionFactory.ConnectAsync(serverInfo).ConfigureAwait(false);

            // Once we have a KuduConnection, register a callback when it's closed,
            // so we can remove it from the connection cache.
            connection.OnDisconnected(RemoveConnection, serverInfo.HostPort);

            return connection;
        }

        private void RemoveConnection(Exception ex, object state)
        {
            var hostPort = (HostAndPort)state;

            lock (_connections)
            {
                // We're here because the connection's OnConnectionClosed
                // was raised. Remove this connection from the cache, so the
                // next time someone requests a connection they get a new one.
                if (_connections.Remove(hostPort))
                {
                    Console.WriteLine($"Connection disconnected, removing from cache {hostPort} {ex}");
                }

                // Note that connections clean themselves up when disconnected,
                // so we don't need to dispose the connection here.
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

        public async ValueTask DisposeAsync()
        {
            List<Task<KuduConnection>> connections;

            lock (_connections)
            {
                connections = _connections.Values.ToList();
                _connections.Clear();
                _disposed = true;
            }

            foreach (var connectionTask in connections)
                await DisposeConnectionTask(connectionTask).ConfigureAwait(false);
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowDisposedException() =>
            throw new ObjectDisposedException(nameof(ConnectionCache));
    }
}
