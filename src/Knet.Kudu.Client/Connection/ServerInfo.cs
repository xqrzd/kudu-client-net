using System.Net;

namespace Knet.Kudu.Client.Connection
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

        public ServerInfo(
            string uuid,
            HostAndPort hostPort,
            IPEndPoint endpoint,
            string location,
            bool isLocal)
        {
            Uuid = uuid;
            HostPort = hostPort;
            Endpoint = endpoint;
            Location = location;
            IsLocal = isLocal;
        }

        /// <summary>
        /// Returns true if the server is in the same location as the given location.
        /// </summary>
        /// <param name="location">The location to check.</param>
        public bool InSameLocation(string location) =>
            !string.IsNullOrEmpty(location) && location.Equals(Location);

        public override string ToString() => $"{Uuid} ({HostPort})";
    }
}
