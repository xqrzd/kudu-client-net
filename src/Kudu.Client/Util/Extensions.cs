using System;
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

            return new ServerInfo(uuid, hostPort);
        }
    }
}
