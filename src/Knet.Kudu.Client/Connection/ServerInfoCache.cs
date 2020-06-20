using System;
using System.Collections.Generic;

namespace Knet.Kudu.Client.Connection
{
    public class ServerInfoCache
    {
        // Use a random number so all clients don't hit the same replicas.
        // The randomInt variable is static so the choice is same across multiple
        // RemoteTablet instances. This ensures follow up calls are routed to the
        // same server with the scanner open.
        private static readonly int _randomInt = new Random().Next(int.MaxValue);

        private readonly List<ServerInfo> _servers;
        private readonly int _leaderIndex;
        private readonly int _randomIndex;

        public ServerInfoCache(List<ServerInfo> servers, int leaderIndex)
        {
            _servers = servers;
            _leaderIndex = leaderIndex;
            _randomIndex = _randomInt % servers.Count;
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
        /// Get the information on the closest server. Servers are ranked from
        /// closest to furthest as follows:
        /// 1) Local servers
        /// 2) Servers in the same location as the client
        /// 3) All other servers
        /// </summary>
        /// <param name="location">The location of the client.</param>
        public ServerInfo GetClosestServerInfo(string location = null)
        {
            // This method returns
            // 1. a randomly picked server among local servers, if there is one based
            //    on IP and assigned location, or
            // 2. a randomly picked server in the same assigned location, if there is a
            //    server in the same location, or, finally,
            // 3. a randomly picked server among all tablet servers.

            var servers = _servers;
            int numServers = servers.Count;
            ServerInfo result = null;
            Span<byte> localServers = stackalloc byte[numServers];
            Span<byte> serversInSameLocation = stackalloc byte[numServers];
            int randomIndex = _randomIndex;
            byte index = 0;
            byte localIndex = 0;
            byte sameLocationIndex = 0;
            bool missingLocation = string.IsNullOrEmpty(location);

            foreach (var serverInfo in servers)
            {
                bool serverInSameLocation = serverInfo.InSameLocation(location);

                // Only consider a server "local" if we're in the same location, or if
                // there is missing location info.
                if (missingLocation || !serverInfo.HasLocation || serverInSameLocation)
                {
                    if (serverInfo.IsLocal)
                        localServers[localIndex++] = index;
                }

                if (serverInSameLocation)
                    serversInSameLocation[sameLocationIndex++] = index;

                if (index == randomIndex)
                    result = serverInfo;

                index++;
            }

            if (localIndex > 0)
            {
                randomIndex = _randomInt % localIndex;
                return servers[localServers[randomIndex]];
            }

            if (sameLocationIndex > 0)
            {
                randomIndex = _randomInt % sameLocationIndex;
                return servers[serversInSameLocation[randomIndex]];
            }

            return result;
        }

        /// <summary>
        /// Helper function to centralize the calling of methods based on the
        /// passed replica selection mechanism.
        /// </summary>
        /// <param name="replicaSelection">Replica selection mechanism to use.</param>
        /// <param name="location">The location of the client.</param>
        public ServerInfo GetServerInfo(ReplicaSelection replicaSelection, string location = null)
        {
            if (replicaSelection == ReplicaSelection.LeaderOnly)
                return GetLeaderServerInfo();

            return GetClosestServerInfo(location);
        }
    }
}
