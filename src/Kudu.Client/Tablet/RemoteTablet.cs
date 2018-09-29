using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kudu.Client.Connection;
using Kudu.Client.Protocol.Consensus;
using Kudu.Client.Protocol.Master;
using Kudu.Client.Util;

namespace Kudu.Client.Tablet
{
    /// <summary>
    /// This class encapsulates the information regarding a tablet and its locations.
    ///
    /// RemoteTablet's main function is to keep track of where the leader for this
    /// tablet is. For example, an RPC might call {@link #getLeaderServerInfo()}, contact that TS, find
    /// it's not the leader anymore, and then call {@link #demoteLeader(String)}.
    ///
    /// A RemoteTablet's life is expected to be long in a cluster where roles aren't changing often,
    /// and short when they do since the Kudu client will replace the RemoteTablet it caches with new
    /// ones after getting tablet locations from the master.
    /// </summary>
    public class RemoteTablet : IEquatable<RemoteTablet>, IComparable<RemoteTablet>
    {
        private readonly Dictionary<string, ServerInfo> _tabletServers;

        public string TableId { get; }

        public string TabletId { get; }

        public Partition Partition { get; }

        public string LeaderUuid { get; }

        public RemoteTablet(
            string tableId,
            string tabletId,
            Partition partition,
            string leaderUuid,
            Dictionary<string, ServerInfo> tabletServers)
        {
            TableId = tableId;
            TabletId = tabletId;
            Partition = partition;
            LeaderUuid = leaderUuid;
            _tabletServers = tabletServers;
        }

        public bool Equals(RemoteTablet other)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(RemoteTablet other)
        {
            throw new NotImplementedException();
        }

        public override bool Equals(object obj) => Equals(obj as RemoteTablet);

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(TabletId).Append("@[");

            var tsStrings = _tabletServers.Values
                .Select(e => $"{e}{(e.Uuid == LeaderUuid ? "[L]" : "")}")
                // Sort so that we have a consistent iteration order.
                .OrderBy(e => e);

            sb.Append(string.Join(',', tsStrings));
            sb.Append(']');
            return sb.ToString();
        }

        public static RemoteTablet FromTabletLocations(
            string tableId, TabletLocationsPB tabletLocations)
        {
            var tabletId = tabletLocations.TabletId.ToStringUtf8();
            var partition = new Partition(
                tabletLocations.Partition.PartitionKeyStart,
                tabletLocations.Partition.PartitionKeyEnd,
                tabletLocations.Partition.HashBuckets);

            var serverInfos = new Dictionary<string, ServerInfo>(
                tabletLocations.Replicas.Count);

            string leaderUuid = null;

            foreach (var replica in tabletLocations.Replicas)
            {
                var uuid = replica.TsInfo.PermanentUuid.ToStringUtf8();

                if (replica.Role == RaftPeerPB.Role.Leader)
                    leaderUuid = uuid;

                var serverInfo = replica.TsInfo.ToServerInfo();
                serverInfos.Add(uuid, serverInfo);
            }

            if (leaderUuid == null)
                Console.WriteLine($"No leader provided for tablet {tabletId}");

            return new RemoteTablet(tableId, tabletId, partition, leaderUuid, serverInfos);
        }
    }
}
