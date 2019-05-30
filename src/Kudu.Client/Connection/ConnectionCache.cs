using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Kudu.Client.Connection
{
    public class ConnectionCache : IDisposable
    {
        private readonly IKuduConnectionFactory _connectionFactory;
        private readonly Dictionary<IPEndPoint, Task<KuduConnection>> _connections;
        private bool _disposed;

        public ConnectionCache(IKuduConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
            _connections = new Dictionary<IPEndPoint, Task<KuduConnection>>();
        }

        public Task<KuduConnection> GetConnectionAsync(
            ServerInfo serverInfo, CancellationToken cancellationToken = default)
        {
            var endpoint = serverInfo.Endpoint;

            lock (_connections)
            {
                if (_disposed)
                    throw new ObjectDisposedException(nameof(ConnectionCache));

                if (_connections.TryGetValue(endpoint, out var connection))
                {
                    // Don't hand out connections we know are faulted, create a new one instead.
                    // The client may still get a faulted connection (if the task hasn't completed yet),
                    // but in that case, the client is responsible for retrying.
                    if (!connection.IsFaulted)
                        return connection;
                }

                var task = ConnectAsync(serverInfo, cancellationToken);
                _connections[endpoint] = task;
                return task;
            }
        }

        private async Task<KuduConnection> ConnectAsync(
            ServerInfo serverInfo, CancellationToken cancellationToken)
        {
            var connection = await _connectionFactory
                .ConnectAsync(serverInfo, cancellationToken).ConfigureAwait(false);

            // Once we have a KuduConnection, register a callback when it's closed,
            // so we can remove it from the connection cache.
            connection.OnDisconnected(RemoveConnection, serverInfo);

            return connection;
        }

        private void RemoveConnection(Exception ex, object state)
        {
            var serverInfo = (ServerInfo)state;

            lock (_connections)
            {
                // We're here because the connection's OnConnectionClosed
                // was raised. Remove this connection from the cache, so the
                // next time someone requests a connection they get a new one.
                if (_connections.Remove(serverInfo.Endpoint))
                {
                    Console.WriteLine($"Connection disconnected, removing from cache {serverInfo.HostPort} {ex}");
                }

                // Note that connections clean themselves up when disconnected,
                // so we don't need to dispose the connection here.
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
            {
                try
                {
                    var connection = await connectionTask.ConfigureAwait(false);
                    await connection.StopAsync().ConfigureAwait(false);
                }
                catch { }
            }
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }
    }
}
