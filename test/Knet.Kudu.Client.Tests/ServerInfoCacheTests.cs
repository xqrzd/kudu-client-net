using System.Collections.Generic;
using System.Net;
using Knet.Kudu.Client.Connection;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class ServerInfoCacheTests
    {
        private static readonly string _clientLocation = "/fake-client";
        private static readonly string _location = "/fake-noclient";
        private static readonly string _noLocation = "";
        private static readonly string[] _uuids = { "uuid-0", "uuid-1", "uuid-2" };

        [Fact]
        public void TestLocalReplica()
        {
            {
                // Let's examine a tablet where the first UUID is local to the client,
                // but no UUID is in the same location.
                var cache = GetCache(0, 0, -1);

                // If the client has no location, we should pick the local server.
                Assert.Equal(_uuids[0], cache.GetClosestServerInfo(_noLocation).Uuid);

                // NOTE: if the client did have a location, because the test replicas are
                // assigned a different default location, they aren't considered local,
                // so we would select one at random.
            }
            {
                // Let's examine a tablet where the first UUID is local to the client,
                // and the second is in the same location.
                var cache = GetCache(0, 0, 1);

                // If the client has no location, we should pick the local server.
                Assert.Equal(_uuids[0], cache.GetClosestServerInfo(_noLocation).Uuid);

                // If the client does have a location, we should pick the one in the same
                // location.
                Assert.Equal(_uuids[1], cache.GetClosestServerInfo(_clientLocation).Uuid);
            }
            {
                // Let's examine a tablet where the first UUID is local to the client and
                // is also in the same location.
                var cache = GetCache(0, 0, 0);

                // If the client has no location, we should pick the local server.
                Assert.Equal(_uuids[0], cache.GetClosestServerInfo(_noLocation).Uuid);

                // If the client does have a location, we should pick the one in the same
                // location.
                Assert.Equal(_uuids[0], cache.GetClosestServerInfo(_clientLocation).Uuid);
            }
        }

        [Fact]
        public void NoLocalOrSameLocationReplica()
        {
            var cache = GetCache(0, -1, -1);

            // We just care about getting one back.
            var info = cache.GetClosestServerInfo(_clientLocation);
            Assert.NotNull(info.Uuid);
        }

        [Fact]
        public void TestReplicaSelection()
        {
            {
                var cache = GetCache(0, 1, 2);

                // LEADER_ONLY picks the leader even if there's a local replica.
                Assert.Equal(_uuids[0],
                    cache.GetServerInfo(ReplicaSelection.LeaderOnly, _clientLocation).Uuid);

                // Since there are locations assigned, CLOSEST_REPLICA picks the replica
                // in the same location, even if there's a local one.
                Assert.Equal(_uuids[2],
                    cache.GetServerInfo(ReplicaSelection.ClosestReplica, _clientLocation).Uuid);
            }
            {
                var cache = GetCache(0, -1, 1);

                // LEADER_ONLY picks the leader even if there's a replica with the same location.
                Assert.Equal(_uuids[0],
                    cache.GetServerInfo(ReplicaSelection.LeaderOnly, _clientLocation).Uuid);

                // CLOSEST_REPLICA picks the replica in the same location.
                Assert.Equal(_uuids[1],
                    cache.GetServerInfo(ReplicaSelection.ClosestReplica, _clientLocation).Uuid);
            }
            {
                var cache = GetCache(0, 1, -1);

                // LEADER_ONLY picks the leader even if there's a local replica.
                Assert.Equal(_uuids[0],
                    cache.GetServerInfo(ReplicaSelection.LeaderOnly, _clientLocation).Uuid);

                // NOTE: the test replicas are assigned a default location. So, even if
                // there are local replicas, because they are in different locations than
                // the client, with CLOSEST_REPLICA, a replica is chosen at random.y
            }
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
            string init = cache.GetClosestServerInfo(_clientLocation).Uuid;
            for (int i = 0; i < 10; i++)
            {
                string next = cache.GetClosestServerInfo(_clientLocation).Uuid;

                Assert.Equal(init, next);
            }
        }

        /// <summary>
        /// Returns a three-replica remote tablet that considers the given indices of
        /// replicas to be leader, local to the client, and in the same location.
        /// </summary>
        private ServerInfoCache GetCache(
            int leaderIndex,
            int localReplicaIndex,
            int sameLocationReplicaIndex)
        {
            var servers = new List<ServerInfo>();

            for (int i = 0; i < 3; i++)
            {
                var uuid = _uuids[i];
                var port = 1000 + i;
                var hostPort = new HostAndPort("host", port);
                var location = i == sameLocationReplicaIndex ? _clientLocation : _location;
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
