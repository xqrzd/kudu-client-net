using System;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Negotiate;
using Microsoft.Extensions.Logging;
using Pipelines.Sockets.Unofficial;

namespace Knet.Kudu.Client.Connection;

public sealed class KuduConnectionFactory : IKuduConnectionFactory
{
    private readonly KuduClientOptions _options;
    private readonly ISecurityContext _securityContext;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly HashSet<IPAddress> _localIPs;

    public KuduConnectionFactory(
        KuduClientOptions options,
        ISecurityContext securityContext,
        ILoggerFactory loggerFactory)
    {
        _options = options;
        _securityContext = securityContext;
        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLogger<KuduConnectionFactory>();
        _localIPs = GetLocalAddresses();
    }

    public async Task<KuduConnection> ConnectAsync(
        ServerInfo serverInfo, CancellationToken cancellationToken = default)
    {
        var endpoint = serverInfo.Endpoint;
        var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        SocketConnection.SetRecommendedClientOptions(socket);

        try
        {
#if NET5_0_OR_GREATER
            await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
#else
            await socket.ConnectAsync(endpoint).ConfigureAwait(false);
#endif

            var negotiator = new Negotiator(_options, _securityContext, _loggerFactory, serverInfo, socket);
            var connection = await negotiator.NegotiateAsync(cancellationToken).ConfigureAwait(false);

            SetTcpKeepAlive(socket);

            return connection;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public async Task<List<ServerInfo>> GetMasterServerInfoAsync(
        HostAndPort hostPort, CancellationToken cancellationToken = default)
    {
        var ipAddresses = await GetHostAddressesAsync(hostPort.Host, cancellationToken)
            .ConfigureAwait(false);
        var servers = new List<ServerInfo>(ipAddresses.Length);

        foreach (var ipAddress in ipAddresses)
        {
            var endpoint = new IPEndPoint(ipAddress, hostPort.Port);
            var isLocal = IsLocal(ipAddress);
            var serverInfo = new ServerInfo("master", hostPort, endpoint, location: null, isLocal);

            servers.Add(serverInfo);
        }

        return servers;
    }

    public async Task<ServerInfo> GetTabletServerInfoAsync(
        HostAndPort hostPort, string uuid, string? location, CancellationToken cancellationToken = default)
    {
        var ipAddresses = await GetHostAddressesAsync(hostPort.Host, cancellationToken)
            .ConfigureAwait(false);
        var ipAddress = ipAddresses[0];

        var endpoint = new IPEndPoint(ipAddress, hostPort.Port);
        var isLocal = IsLocal(ipAddress);

        return new ServerInfo(uuid, hostPort, endpoint, location, isLocal);
    }

    private bool IsLocal(IPAddress ipAddress)
    {
        return IPAddress.IsLoopback(ipAddress) || _localIPs.Contains(ipAddress);
    }

    private static async Task<IPAddress[]> GetHostAddressesAsync(string hostName, CancellationToken cancellationToken)
    {
        Exception? exception = null;

        try
        {
#if NET6_0_OR_GREATER
            var ipAddresses = await Dns.GetHostAddressesAsync(hostName, cancellationToken).ConfigureAwait(false);
#else
            var ipAddresses = await Dns.GetHostAddressesAsync(hostName).ConfigureAwait(false);
#endif

            if (ipAddresses.Length > 0)
            {
                return ipAddresses;
            }
        }
        catch (Exception ex)
        {
            exception = ex;
        }

        var status = KuduStatus.IOError($"Unable to resolve address for {hostName}");
        throw new NonRecoverableException(status, exception);
    }

    private static HashSet<IPAddress> GetLocalAddresses()
    {
        var addresses = new HashSet<IPAddress>();

        // Dns.GetHostAddresses(Dns.GetHostName()) returns incomplete results on Linux.
        // https://github.com/dotnet/runtime/issues/27534
        foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
        {
            foreach (var ipInfo in networkInterface.GetIPProperties().UnicastAddresses)
            {
                addresses.Add(ipInfo.Address);
            }
        }

        return addresses;
    }

    private void SetTcpKeepAlive(Socket socket)
    {
        try
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

#if NETCOREAPP3_1_OR_GREATER
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 60);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 3);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 10);
#endif
        }
        catch (Exception ex)
        {
            _logger.SetTcpKeepAliveFailed(ex);
        }
    }
}
