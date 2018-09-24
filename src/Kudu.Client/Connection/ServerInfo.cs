namespace Kudu.Client.Connection
{
    /// <summary>
    /// Container class for server information that never changes, like UUID and hostname.
    /// </summary>
    public class ServerInfo
    {
        public string Uuid { get; }

        public HostAndPort HostPort { get; }

        public ServerInfo(string uuid, HostAndPort hostPort)
        {
            Uuid = uuid;
            HostPort = hostPort;
        }

        public override string ToString() => $"{Uuid} ({HostPort})";
    }
}
