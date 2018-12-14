using System;
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

        public ServerInfo GetLeaderServerInfo()
        {
            // Check if we have a leader.
            if (_leaderIndex == -1)
                return null;

            return _servers[_leaderIndex];
        }

        public ServerInfo GetClosestServerInfo()
        {
            ServerInfo last = null;

            foreach (var server in _servers)
            {
                last = server;

                if (server.IsLocal)
                    break;
            }

            return last;
        }

        public ServerInfo GetServerInfo(ReplicaSelection replicaSelection)
        {
            switch (replicaSelection)
            {
                case ReplicaSelection.LeaderOnly:
                    return GetLeaderServerInfo();

                case ReplicaSelection.ClosestReplica:
                    return GetClosestServerInfo();

                default:
                    throw new NotSupportedException($"Unknown replica selection {replicaSelection}");
            }
        }
    }
}
