using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Connection;

public sealed class ConnectionCache : IAsyncDisposable
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
            ThrowObjectDisposedException();

        IPEndPoint endpoint = serverInfo.Endpoint;
        Task<KuduConnection>? connectionTask;
        bool newConnection = false;

        lock (_connections)
        {
            if (!_connections.TryGetValue(endpoint, out connectionTask))
            {
                connectionTask = _connectionFactory.ConnectAsync(serverInfo, cancellationToken);
                _connections.Add(endpoint, connectionTask);
                newConnection = true;
            }
        }

        try
        {
            var connection = await connectionTask.ConfigureAwait(false);

            if (newConnection)
            {
                connection.ConnectionClosed.UnsafeRegister(
                    state => RemoveConnection((IPEndPoint)state!),
                    endpoint);
            }

            return connection;
        }
        catch (Exception ex)
        {
            // Failed to negotiate a new connection.
            RemoveFaultedConnection(endpoint);

            _logger.UnableToConnectToServer(ex, serverInfo);

            if (ex is NonRecoverableException)
            {
                // Always retry, except for these.
                throw;
            }

            // The upper-level caller should handle the exception and
            // retry using a new connection.
            throw new RecoverableException(
                KuduStatus.NetworkError(ex.Message), ex);
        }
    }

    private void RemoveFaultedConnection(IPEndPoint endpoint)
    {
        lock (_connections)
        {
            if (_connections.TryGetValue(endpoint, out var connectionTask))
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

    private void RemoveConnection(IPEndPoint endpoint)
    {
        lock (_connections)
        {
            _connections.Remove(endpoint);
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
                await connection.DisposeAsync().ConfigureAwait(false);
            }
            catch { }
        }
    }

    [DoesNotReturn]
    private static void ThrowObjectDisposedException() =>
        throw new ObjectDisposedException(nameof(ConnectionCache));
}
