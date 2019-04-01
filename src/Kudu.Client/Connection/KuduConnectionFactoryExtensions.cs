using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kudu.Client.Protocol.Consensus;
using Kudu.Client.Protocol.Master;
using Kudu.Client.Tablet;
using Kudu.Client.Util;

namespace Kudu.Client.Connection
{
    public static class KuduConnectionFactoryExtensions
    {
        public static Task<ServerInfo> GetServerInfoAsync(
            this IKuduConnectionFactory connectionFactory,
            TSInfoPB tsInfo)
        {
            string uuid = tsInfo.PermanentUuid.ToStringUtf8();
            string location = tsInfo.Location;
            var addresses = tsInfo.RpcAddresses;

            if (addresses.Count == 0)
            {
                Console.WriteLine($"Received a tablet server with no addresses {uuid}");
                return Task.FromResult<ServerInfo>(null);
            }

            // TODO: if the TS advertises multiple host/ports, pick the right one
            // based on some kind of policy. For now just use the first always.
            var hostPort = addresses[0].ToHostAndPort();

            return connectionFactory.GetServerInfoAsync(uuid, location, hostPort);
        }

        public static async Task<RemoteTablet> GetTabletAsync(
            this IKuduConnectionFactory connectionFactory,
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
                var serverInfo = await connectionFactory.GetServerInfoAsync(replica.TsInfo)
                    .ConfigureAwait(false);

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
