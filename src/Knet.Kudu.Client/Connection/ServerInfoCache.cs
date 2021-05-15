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

            var numServers = servers.Count;
            if (numServers > 0)
                _randomIndex = _randomInt % numServers;
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
        /// Select the closest replica to the client. Replicas are classified
        /// from closest to furthest as follows:
        /// <list type="number">
        /// <item><description>Local replicas</description></item>
        /// <item><description>
        /// Replicas whose tablet server has the same location as the client
        /// </description></item>
        /// <item><description>All other replicas</description></item>
        /// </list>
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

        /// <summary>
        /// Clears the leader UUID if the passed tablet server is the current leader.
        /// If it is the current leader, then the next call to this tablet will have
        /// to query the master to find the new leader.
        /// </summary>
        /// <param name="uuid">
        /// A tablet server that gave a sign that it isn't this tablet's leader.
        /// </param>
        public ServerInfoCache DemoteLeader(string uuid)
        {
            if (_leaderIndex == -1)
            {
                // There is no leader for this tablet.
                return this;
            }

            return new ServerInfoCache(_servers, -1);
        }

        /// <summary>
        /// Removes the passed tablet server from this tablet's list of tablet servers.
        /// </summary>
        /// <param name="uuid">A tablet server to remove from this cache.</param>
        public ServerInfoCache RemoveTabletServer(string uuid)
        {
            var servers = _servers;
            var numServers = servers.Count;
            var serverToRemove = -1;

            for (int i = 0; i < numServers; i++)
            {
                var server = servers[i];
                if (server.Uuid == uuid)
                {
                    serverToRemove = i;
                    break;
                }
            }

            if (serverToRemove == -1)
                return this;

            var numNewServers = numServers - 1;
            var newServers = new List<ServerInfo>(numNewServers);
            for (int i = 0; i < numNewServers; i++)
            {
                if (i == serverToRemove)
                    continue;

                newServers.Add(servers[i]);
            }

            var leaderIndex = _leaderIndex;
            if (leaderIndex == serverToRemove)
            {
                leaderIndex = -1;
            }
            else if (leaderIndex > serverToRemove)
            {
                leaderIndex--;
            }

            return new ServerInfoCache(newServers, leaderIndex);
        }
    }
}
