using System;
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

        public static async Task<List<RemoteTablet>> GetTabletsAsync(
            this IKuduConnectionFactory connectionFactory,
            string tableId, GetTableLocationsResponsePB locations)
        {
            // TODO: Need error handling here.
            var tsInfos = locations.TsInfos;
            var internedServers = new List<ServerInfo>(tsInfos.Count);
            var results = new List<RemoteTablet>(locations.TabletLocations.Count);

            foreach (var tsInfo in tsInfos)
            {
                var serverInfo = await GetServerInfoAsync(connectionFactory, tsInfo)
                    .ConfigureAwait(false);

                internedServers.Add(serverInfo);
            }

            foreach (var tabletInfo in locations.TabletLocations)
            {
                var tabletId = tabletInfo.TabletId.ToStringUtf8();
                var partition = new Partition(
                    tabletInfo.Partition.PartitionKeyStart,
                    tabletInfo.Partition.PartitionKeyEnd,
                    tabletInfo.Partition.HashBuckets);

                var numReplicas = Math.Max(
                    tabletInfo.DEPRECATEDreplicas.Count,
                    tabletInfo.InternedReplicas.Count);

                var servers = new List<ServerInfo>(numReplicas);
                var replicas = new List<Replica>(numReplicas);
                int leaderIndex = -1;

                // Handle interned replicas.
                foreach (var replicaPb in tabletInfo.InternedReplicas)
                {
                    var tsInfoIdx = (int)replicaPb.TsInfoIdx;
                    var serverInfo = internedServers[tsInfoIdx];

                    var replica = new Replica(
                        serverInfo.HostPort,
                        replicaPb.Role,
                        replicaPb.DimensionLabel);

                    if (replica.Role == RaftPeerPB.Role.Leader)
                        leaderIndex = servers.Count;

                    servers.Add(serverInfo);
                    replicas.Add(replica);
                }

                // Handle "old-style" non-interned replicas.
                // It's used for backward compatibility.
                foreach (var replicaPb in tabletInfo.DEPRECATEDreplicas)
                {
                    var serverInfo = await connectionFactory.GetServerInfoAsync(
                        replicaPb.TsInfo).ConfigureAwait(false);

                    if (serverInfo != null)
                    {
                        var replica = new Replica(
                            serverInfo.HostPort,
                            replicaPb.Role,
                            replicaPb.DimensionLabel);

                        if (replica.Role == RaftPeerPB.Role.Leader)
                            leaderIndex = servers.Count;

                        servers.Add(serverInfo);
                        replicas.Add(replica);
                    }
                }

                var serverCache = new ServerInfoCache(servers, leaderIndex);

                var tablet = new RemoteTablet(
                    tableId,
                    tabletId,
                    partition,
                    serverCache,
                    replicas);

                results.Add(tablet);
            }

            return results;
        }
    }
}
