using System.Net;

namespace Kudu.Client.Connection
{
    /// <summary>
    /// Container class for server information that never changes, like UUID and hostname.
    /// </summary>
    public class ServerInfo
    {
        public string Uuid { get; }

        public HostAndPort HostPort { get; }

        public IPEndPoint Endpoint { get; }

        public bool IsLocal { get; }

        public ServerInfo(string uuid, HostAndPort hostPort, IPEndPoint endpoint, bool isLocal)
        {
            Uuid = uuid;
            HostPort = hostPort;
            Endpoint = endpoint;
            IsLocal = IsLocal;
        }

        public override string ToString() => $"{Uuid} ({HostPort})";
    }
}
