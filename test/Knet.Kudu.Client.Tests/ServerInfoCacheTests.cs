using System.Collections.Generic;
using System.Net;
using Knet.Kudu.Client.Connection;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class ServerInfoCacheTests
    {
        private static readonly string ClientLocation = "/fake-client";
        private static readonly string Location = "/fake-noclient";
        private static readonly string NoLocation = "";
        private static readonly string[] Uuids = { "uuid-0", "uuid-1", "uuid-2" };

        [Fact]
        public void LocalReplicaNotSameLocation()
        {
            // Tablet with no replicas in the same location as the client.
            var cache = GetCache(0, 0, -1);

            // No location for the client.
            Assert.Equal(Uuids[0], cache.GetClosestServerInfo(NoLocation).Uuid);

            // Client with location.
            Assert.Equal(Uuids[0], cache.GetClosestServerInfo(ClientLocation).Uuid);
        }

        [Fact]
        public void NonLocalReplicaSameLocation()
        {
            // Tablet with a non-local replica in the same location as the client.
            var cache = GetCache(0, 0, 1);

            // No location for the client.
            Assert.Equal(Uuids[0], cache.GetClosestServerInfo(NoLocation).Uuid);

            // Client with location. The local replica should be chosen.
            Assert.Equal(Uuids[0], cache.GetClosestServerInfo(ClientLocation).Uuid);
        }

        [Fact]
        public void LocalReplicaSameLocation()
        {
            // Tablet with a local replica in the same location as the client.
            var cache = GetCache(0, 0, 0);

            // No location for the client.
            Assert.Equal(Uuids[0], cache.GetClosestServerInfo(NoLocation).Uuid);

            // Client with location. The local replica should be chosen.
            Assert.Equal(Uuids[0], cache.GetClosestServerInfo(ClientLocation).Uuid);
        }

        [Fact]
        public void NoLocalOrSameLocationReplica()
        {
            var cache = GetCache(0, -1, -1);

            // We just care about getting one back.
            var info = cache.GetClosestServerInfo(ClientLocation);
            Assert.NotNull(info.Uuid);
        }

        [Fact]
        public void LocalReplica()
        {
            var cache = GetCache(0, 1, 2);

            // LEADER_ONLY picks the leader even if there's a local replica.
            Assert.Equal(Uuids[0],
                cache.GetServerInfo(ReplicaSelection.LeaderOnly, ClientLocation).Uuid);

            // CLOSEST_REPLICA picks the local replica even if there's a replica in the same location.
            Assert.Equal(Uuids[1],
                cache.GetServerInfo(ReplicaSelection.ClosestReplica, ClientLocation).Uuid);
        }

        [Fact]
        public void NoLocalReplica()
        {
            var cache = GetCache(0, -1, 1);

            // LEADER_ONLY picks the leader even if there's a replica with the same location.
            Assert.Equal(Uuids[0],
                cache.GetServerInfo(ReplicaSelection.LeaderOnly, ClientLocation).Uuid);

            // CLOSEST_REPLICA picks the replica in the same location.
            Assert.Equal(Uuids[1],
                cache.GetServerInfo(ReplicaSelection.ClosestReplica, ClientLocation).Uuid);
        }

        [Fact]
        public void TestGetReplicaSelectedServerInfoDeterminism()
        {
            // There's a local leader replica.
            var tabletWithLocal = GetCache(0, 0, 0);
            VerifyGetReplicaSelectedServerInfoDeterminism(tabletWithLocal);

            // There's a leader in the same location as the client.
            var tabletWithSameLocation = GetCache(0, -1, 0);
            VerifyGetReplicaSelectedServerInfoDeterminism(tabletWithSameLocation);

            // There's no local replica or replica in the same location.
            var tabletWithRemote = GetCache(0, -1, -1);
            VerifyGetReplicaSelectedServerInfoDeterminism(tabletWithRemote);
        }

        private void VerifyGetReplicaSelectedServerInfoDeterminism(ServerInfoCache cache)
        {
            string init = cache.GetClosestServerInfo(ClientLocation).Uuid;
            for (int i = 0; i < 10; i++)
            {
                string next = cache.GetClosestServerInfo(ClientLocation).Uuid;

                Assert.Equal(init, next);
            }
        }

        private ServerInfoCache GetCache(
            int leaderIndex,
            int localReplicaIndex,
            int sameLocationReplicaIndex)
        {
            var servers = new List<ServerInfo>();

            for (int i = 0; i < 2; i++)
            {
                var uuid = Uuids[i];
                var port = 1000 + i;
                var hostPort = new HostAndPort("host", port);
                var location = i == sameLocationReplicaIndex ? ClientLocation : Location;
                var local = i == localReplicaIndex;
                var endpoint = i == localReplicaIndex ?
                    new IPEndPoint(IPAddress.Parse("127.0.0.1"), port) :
                    new IPEndPoint(IPAddress.Parse("1.2.3.4"), port);

                servers.Add(new ServerInfo(uuid, hostPort, endpoint, location, local));
            }

            return new ServerInfoCache(servers, leaderIndex);
        }
    }
}
