using System;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using Kudu.Client.Connection;
using Kudu.Client.Protocol;
using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Util
{
    public static class Extensions
    {
        public static string ToStringUtf8(this byte[] source) =>
            Encoding.UTF8.GetString(source);

        public static byte[] ToUtf8ByteArray(this string source) =>
            Encoding.UTF8.GetBytes(source);

        public static HostAndPort ToHostAndPort(this HostPortPB hostPort) =>
            new HostAndPort(hostPort.Host, (int)hostPort.Port);

        public static void SwapMostSignificantBitBigEndian(this byte[] span) =>
            span[0] ^= 1 << 7;

        public static void SwapMostSignificantBitBigEndian(this Span<byte> span) =>
            span[0] ^= 1 << 7;

        public static int GetContentHashCode(this byte[] source)
        {
            if (source == null)
                return 0;

            int result = 1;
            foreach (byte element in source)
                result = 31 * result + element;

            return result;
        }

        public static ServerInfo ToServerInfo(this TSInfoPB tsInfo)
        {
            string uuid = tsInfo.PermanentUuid.ToStringUtf8();
            var addresses = tsInfo.RpcAddresses;

            if (addresses.Count == 0)
            {
                Console.WriteLine($"Received a tablet server with no addresses {uuid}");
                return null;
            }

            // TODO: if the TS advertises multiple host/ports, pick the right one
            // based on some kind of policy. For now just use the first always.
            var hostPort = addresses[0].ToHostAndPort();

            return hostPort.CreateServerInfo(uuid);
        }

        public static ServerInfo CreateServerInfo(this HostAndPort hostPort, string uuid)
        {
            var ipAddresses = Dns.GetHostAddresses(hostPort.Host);
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
            var isLocal = ipAddress.IsLocal();

            Console.WriteLine($"Endpoint: {endpoint} [local: {isLocal}]");

            return new ServerInfo(uuid, hostPort, endpoint, isLocal);
        }

        public static bool IsLocal(this IPAddress ipAddress)
        {
            List<IPAddress> localIPs = GetLocalAddresses();
            return IPAddress.IsLoopback(ipAddress) || localIPs.Contains(ipAddress);
        }

        private static List<IPAddress> GetLocalAddresses()
        {
            var addresses = new List<IPAddress>();

            // Dns.GetHostAddresses(Dns.GetHostName()) returns incomplete results on Linux.
            // https://github.com/dotnet/corefx/issues/32611
            foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
            {
                foreach (var ip in networkInterface.GetIPProperties().UnicastAddresses)
                {
                    addresses.Add(ip.Address);
                }
            }

            return addresses;
        }
    }
}
