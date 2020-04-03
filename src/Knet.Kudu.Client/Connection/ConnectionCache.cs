using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Logging;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Connection
{
    public class ConnectionCache : IAsyncDisposable
    {
        private readonly IKuduConnectionFactory _connectionFactory;
        private readonly ILogger _logger;
        private readonly Dictionary<IPEndPoint, Task<KuduConnection>> _connections;
        private volatile bool _disposed;

        public ConnectionCache(
            IKuduConnectionFactory connectionFactory,
            ILoggerFactory loggerFactory)
        {
            _connectionFactory = connectionFactory;
            _logger = loggerFactory.CreateLogger<ConnectionCache>();
            _connections = new Dictionary<IPEndPoint, Task<KuduConnection>>();
        }

        public async Task<KuduConnection> GetConnectionAsync(
            ServerInfo serverInfo, CancellationToken cancellationToken = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ConnectionCache));

            IPEndPoint endpoint = serverInfo.Endpoint;
            Task<KuduConnection> connectionTask;

            lock (_connections)
            {
                if (!_connections.TryGetValue(endpoint, out connectionTask))
                {
                    connectionTask = ConnectAsync(serverInfo, cancellationToken);
                    _connections.Add(endpoint, connectionTask);
                }
            }

            try
            {
                return await connectionTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Failed to negotiate a new connection.
                RemoveFaultedConnection(serverInfo, skipTaskStatusCheck: false);

                _logger.UnableToConnectToServer(serverInfo, ex);

                if (ex is NonRecoverableException)
                {
                    // Always retry, except for these.
                    throw;
                }

                // The upper-level caller should handle the exception and
                // retry using a new connection.
                throw new RecoverableException(
                    KuduStatus.ServiceUnavailable(ex.Message), ex);
            }
        }

        private async Task<KuduConnection> ConnectAsync(
            ServerInfo serverInfo, CancellationToken cancellationToken = default)
        {
            KuduConnection connection = await _connectionFactory
                .ConnectAsync(serverInfo, cancellationToken).ConfigureAwait(false);

            connection.OnDisconnected((exception, state) => RemoveFaultedConnection(
                (ServerInfo)state, skipTaskStatusCheck: true), serverInfo);

            return connection;
        }

        private void RemoveFaultedConnection(ServerInfo serverInfo, bool skipTaskStatusCheck)
        {
            IPEndPoint endpoint = serverInfo.Endpoint;

            lock (_connections)
            {
                if (skipTaskStatusCheck)
                {
                    _connections.Remove(endpoint);
                }
                else
                {
                    if (_connections.TryGetValue(endpoint, out Task<KuduConnection> connectionTask))
                    {
                        // Someone else might have already replaced the faulted connection.
                        // Confirm that the connection we're about to remove is faulted.
                        if (connectionTask.IsFaulted)
                        {
                            _connections.Remove(endpoint);
                        }
                    }
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            _disposed = true;
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
                    // TODO: Cancellation token support so we can cancel
                    // any connections still in the negotiation phase?
                    KuduConnection connection = await connectionTask.ConfigureAwait(false);
                    await connection.StopAsync().ConfigureAwait(false);
                }
                catch { }
            }
        }
    }
}
