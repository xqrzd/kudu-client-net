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
    /// tablet is. For example, an RPC might call GetServerInfo, contact that TS, find
    /// it's not the leader anymore, and then call DemoteLeader.
    ///
    /// A RemoteTablet's life is expected to be long in a cluster where roles aren't changing often,
    /// and short when they do since the Kudu client will replace the RemoteTablet it caches with new
    /// ones after getting tablet locations from the master.
    /// </summary>
    public class RemoteTablet : IEquatable<RemoteTablet>
    {
        private readonly ServerInfoCache _cache;

        public string TableId { get; }

        public string TabletId { get; }

        public Partition Partition { get; }

        public RemoteTablet(
            string tableId,
            string tabletId,
            Partition partition,
            ServerInfoCache cache)
        {
            TableId = tableId;
            TabletId = tabletId;
            Partition = partition;
            _cache = cache;
        }

        public ServerInfo GetServerInfo(ReplicaSelection replicaSelection) =>
            _cache.GetServerInfo(replicaSelection);

        public bool Equals(RemoteTablet other)
        {
            if (other is null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return TabletId == other.TabletId;
        }

        public override bool Equals(object obj) =>
            Equals(obj as RemoteTablet);

        public override int GetHashCode() =>
            TabletId.GetHashCode();

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(TabletId).Append("@[");

            var leader = _cache.GetServerInfo(ReplicaSelection.LeaderOnly);

            var tsStrings = _cache.Servers
                .Select(e => $"{e}{(e == leader ? "[L]" : "")}")
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

            var replicas = new List<ServerInfo>(tabletLocations.Replicas.Count);
            int leaderIndex = -1;

            for (int i = 0; i < tabletLocations.Replicas.Count; i++)
            {
                var replica = tabletLocations.Replicas[i];
                var serverInfo = replica.TsInfo.ToServerInfo();

                if (replica.Role == RaftPeerPB.Role.Leader)
                    leaderIndex = i;

                replicas.Add(serverInfo);
            }

            if (leaderIndex == -1)
                Console.WriteLine($"No leader provided for tablet {tabletId}");

            var cache = new ServerInfoCache(replicas, leaderIndex);

            return new RemoteTablet(tableId, tabletId, partition, cache);
        }
    }
}
