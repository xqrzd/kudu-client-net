using System;
using System.Collections.Generic;

namespace Kudu.Client.Connection
{
    public class ServerInfoCache
    {
        private readonly List<ServerInfo> _servers;
        private readonly int _leaderIndex;

        public ServerInfoCache(List<ServerInfo> servers, int leaderIndex)
        {
            _servers = servers;
            _leaderIndex = leaderIndex;
        }

        /// <summary>
        /// Get replicas of this tablet.
        /// </summary>
        public IReadOnlyList<ServerInfo> Servers => _servers;

        /// <summary>
        /// Get the information on the tablet server that we think holds the
        /// leader replica for this tablet. Returns null if we don't know who
        /// the leader is.
        /// </summary>
        public ServerInfo GetLeaderServerInfo()
        {
            // Check if we have a leader.
            if (_leaderIndex == -1)
                return null;

            return _servers[_leaderIndex];
        }

        /// <summary>
        /// Get the information on the closest server. If none is closer than
        /// the others, return the information on a randomly picked server.
        /// Returns null if this cache doesn't know any servers.
        /// </summary>
        /// <param name="location">The location of the client.</param>
        public ServerInfo GetClosestServerInfo(string location = null)
        {
            ServerInfo last = null;
            ServerInfo lastInSameLocation = null;

            foreach (var server in _servers)
            {
                last = server;

                if (server.IsLocal)
                    return server;

                if (server.InSameLocation(location))
                    lastInSameLocation = server;
            }

            if (lastInSameLocation != null)
                return lastInSameLocation;

            return last;
        }

        /// <summary>
        /// Helper function to centralize the calling of methods based on the
        /// passed replica selection mechanism.
        /// </summary>
        /// <param name="replicaSelection">Replica selection mechanism to use.</param>
        /// <param name="location">The location of the client.</param>
        public ServerInfo GetServerInfo(ReplicaSelection replicaSelection, string location = null)
        {
            switch (replicaSelection)
            {
                case ReplicaSelection.LeaderOnly:
                    return GetLeaderServerInfo();

                case ReplicaSelection.ClosestReplica:
                    return GetClosestServerInfo(location);

                default:
                    throw new NotSupportedException($"Unknown replica selection {replicaSelection}");
            }
        }
    }
}
