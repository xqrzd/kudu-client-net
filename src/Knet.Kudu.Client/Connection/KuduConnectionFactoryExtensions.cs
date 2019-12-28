using System.Collections.Generic;
using System.Threading.Tasks;
using Knet.Kudu.Client.Protocol.Consensus;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Connection
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

            foreach (var replica in tabletLocations.Replicas)
            {
                var serverInfo = await connectionFactory.GetServerInfoAsync(replica.TsInfo)
                    .ConfigureAwait(false);

                if (serverInfo != null)
                {
                    if (replica.Role == RaftPeerPB.Role.Leader)
                        leaderIndex = replicas.Count;

                    replicas.Add(serverInfo);
                }
            }

            var cache = new ServerInfoCache(replicas, leaderIndex);

            return new RemoteTablet(tableId, tabletId, partition, cache);
        }
    }
}
