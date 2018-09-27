using System.Collections.Generic;
using System.Linq;

namespace Kudu.Client.Connection
{
    public class MasterCache
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
    }
}
