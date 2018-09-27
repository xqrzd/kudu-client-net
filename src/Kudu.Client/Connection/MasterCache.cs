using System.Collections.Generic;
using System.Linq;

namespace Kudu.Client.Connection
{
    public class MasterCache
    {
        private readonly List<ServerInfo> _masters;
        private readonly int _leaderIndex;

        public MasterCache(IReadOnlyList<ServerInfo> masters, int leaderIndex)
        {
            _masters = masters.ToList();
            _leaderIndex = leaderIndex;
        }

        public ServerInfo GetMasterInfo(HostAndPort hostPort, ReplicaSelection replicaSelection)
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
    }
}
