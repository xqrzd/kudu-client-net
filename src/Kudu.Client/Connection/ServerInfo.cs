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

        public string Location { get; }

        public bool IsLocal { get; }

        public ServerInfo(string uuid, HostAndPort hostPort, IPEndPoint endpoint, string location, bool isLocal)
        {
            Uuid = uuid;
            HostPort = hostPort;
            Endpoint = endpoint;
            Location = location;
            IsLocal = isLocal;
        }

        public override string ToString() => $"{Uuid} ({HostPort})";
    }
}
