using System.Collections.Generic;

namespace Kudu.Client.Connection
{
    public class ServerInfoCache
    {
        private readonly List<ServerInfo> _servers;
        private readonly int _leaderIndex;

        public IReadOnlyList<ServerInfo> Servers => _servers.AsReadOnly();

        public ServerInfoCache(List<ServerInfo> servers, int leaderIndex)
        {
            _servers = servers;
            _leaderIndex = leaderIndex;
        }

        public ServerInfo GetServerInfo(ReplicaSelection replicaSelection)
        {
            if (replicaSelection == ReplicaSelection.LeaderOnly)
            {
                return _servers[_leaderIndex];
            }
            else
            {
                // TODO: Add location awareness.
                return _servers[0];
            }
        }
    }
}
