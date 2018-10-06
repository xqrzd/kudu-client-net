using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Kudu.Client.Connection
{
    // TODO: Rename this
    public class MasterCache : IEnumerable<HostAndPort>
    {
        private readonly List<HostAndPort> _masters;
        private readonly int _leaderIndex;

        public MasterCache(List<HostAndPort> masters, int leaderIndex)
        {
            _masters = masters.ToList();
            _leaderIndex = leaderIndex;
        }

        public HostAndPort GetMasterInfo(ReplicaSelection replicaSelection)
        {
            if (replicaSelection == ReplicaSelection.LeaderOnly)
            {
                return _masters[_leaderIndex];
            }
            else
            {
                // TODO: Add location awareness.
                return _masters[0];
            }
        }

        public IEnumerator<HostAndPort> GetEnumerator() =>
            _masters.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

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
