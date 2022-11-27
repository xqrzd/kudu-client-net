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
    private readonly CancellationTokenSource _disposedCts;
    private bool _disposed;

    public ConnectionCache(
        IKuduConnectionFactory connectionFactory,
        ILoggerFactory loggerFactory)
    {
        _connectionFactory = connectionFactory;
        _logger = loggerFactory.CreateLogger<ConnectionCache>();
        _connections = new Dictionary<IPEndPoint, Task<KuduConnection>>();
        _disposedCts = new CancellationTokenSource();
    }

    public async Task<KuduConnection> GetConnectionAsync(
        ServerInfo serverInfo, CancellationToken cancellationToken = default)
    {
        IPEndPoint endpoint = serverInfo.Endpoint;
        Task<KuduConnection>? connectionTask;
        var newConnection = false;
        var stoppingToken = _disposedCts.Token;

        lock (_connections)
        {
            if (_disposed)
                ThrowObjectDisposedException();

            if (!_connections.TryGetValue(endpoint, out connectionTask))
            {
                // Don't cancel the connect task if the requesting RPC was cancelled.
                // This prevents unnecessary connects if RPCs are configured with short
                // timeouts. Only abort the connect task when the client is disposed.
                connectionTask = _connectionFactory.ConnectAsync(serverInfo, stoppingToken);
                _connections.Add(endpoint, connectionTask);
                newConnection = true;
            }
        }

        try
        {
            var connection = await connectionTask
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

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
                // The faulted connection may have already been replaced.
                // Confirm the connection we're about to remove is faulted.
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
        List<Task<KuduConnection>>? connections = null;

        lock (_connections)
        {
            if (!_disposed)
            {
                connections = _connections.Values.ToList();
                _connections.Clear();
                _disposedCts.Cancel();

                _disposed = true;
            }
        }

        if (connections is not null)
        {
            foreach (var connectionTask in connections)
            {
                try
                {
                    KuduConnection connection = await connectionTask.ConfigureAwait(false);
                    await connection.DisposeAsync().ConfigureAwait(false);
                }
                catch { }
            }
        }

        _disposedCts.Dispose();
    }

    [DoesNotReturn]
    private static void ThrowObjectDisposedException() =>
        throw new ObjectDisposedException(nameof(ConnectionCache));
}
