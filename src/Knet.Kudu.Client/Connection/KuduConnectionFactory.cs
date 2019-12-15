using System;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Negotiate;
using Pipelines.Sockets.Unofficial;

namespace Knet.Kudu.Client.Connection
{
    public class KuduConnectionFactory : IKuduConnectionFactory
    {
        private readonly KuduClientOptions _options;
        private readonly HashSet<IPAddress> _localIPs;

        public KuduConnectionFactory(KuduClientOptions options)
        {
            _options = options;
            _localIPs = GetLocalAddresses();
        }

        public async Task<KuduConnection> ConnectAsync(
            ServerInfo serverInfo, CancellationToken cancellationToken = default)
        {
            var socket = await ConnectAsync(serverInfo.Endpoint).ConfigureAwait(false);

            var negotiator = new Negotiator(_options, serverInfo, socket);
            return await negotiator.NegotiateAsync(cancellationToken).ConfigureAwait(false);
        }

        public async Task<ServerInfo> GetServerInfoAsync(string uuid, string location, HostAndPort hostPort)
        {
            var ipAddresses = await Dns.GetHostAddressesAsync(hostPort.Host).ConfigureAwait(false);
            if (ipAddresses == null || ipAddresses.Length == 0)
                throw new Exception($"Failed to resolve the IP of '{hostPort.Host}'");

            var ipAddress = ipAddresses[0];
            if (ipAddress.AddressFamily != AddressFamily.InterNetwork)
            {
                // Prefer an IPv4 address.
                for (int i = 1; i < ipAddresses.Length; i++)
                {
                    var newAddress = ipAddresses[i];
                    if (newAddress.AddressFamily == AddressFamily.InterNetwork)
                    {
                        ipAddress = newAddress;
                        break;
                    }
                }
            }

            var endpoint = new IPEndPoint(ipAddress, hostPort.Port);
            var isLocal = IsLocal(ipAddress);

            return new ServerInfo(uuid, hostPort, endpoint, location, isLocal);
        }

        private bool IsLocal(IPAddress ipAddress)
        {
            return IPAddress.IsLoopback(ipAddress) || _localIPs.Contains(ipAddress);
        }

        private static async Task<Socket> ConnectAsync(IPEndPoint endpoint)
        {
            var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            SocketConnection.SetRecommendedClientOptions(socket);

            await socket.ConnectAsync(endpoint).ConfigureAwait(false);
            return socket;
        }

        private static HashSet<IPAddress> GetLocalAddresses()
        {
            var addresses = new HashSet<IPAddress>();

            // Dns.GetHostAddresses(Dns.GetHostName()) returns incomplete results on Linux.
            // https://github.com/dotnet/corefx/issues/32611
            foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
            {
                foreach (var ipInfo in networkInterface.GetIPProperties().UnicastAddresses)
                {
                    addresses.Add(ipInfo.Address);
                }
            }

            return addresses;
        }
    }
}
