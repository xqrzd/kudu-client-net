using System;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client.Tablet;

/// <summary>
/// <para>
/// This class encapsulates the information regarding a tablet and its locations.
/// </para>
/// 
/// <para>
/// RemoteTablet's main function is to keep track of where the leader for this
/// tablet is. For example, an RPC might call GetServerInfo, contact that TS, find
/// it's not the leader anymore, and then re-fetch the tablet locations. This
/// class is immutable.
/// </para>
/// 
/// <para>
/// A RemoteTablet's life is expected to be long in a cluster where roles aren't
/// changing often, and short when they do since the Kudu client will replace the
/// RemoteTablet it caches with new ones after getting tablet locations from the master.
/// </para>
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

    public IReadOnlyList<ServerInfo> Servers => _cache.Servers;

    public IReadOnlyList<KuduReplica> Replicas => _cache.Replicas;

    public ServerInfo? GetServerInfo(
        ReplicaSelection replicaSelection, string? location = null)
    {
        return _cache.GetServerInfo(replicaSelection, location);
    }

    public ServerInfo? GetLeaderServerInfo() => _cache.GetLeaderServerInfo();

    /// <summary>
    /// Return the current leader, or null if there is none.
    /// </summary>
    public KuduReplica? GetLeaderReplica() => _cache.GetLeaderReplica();

    public RemoteTablet DemoteLeader(string uuid)
    {
        var cache = _cache.DemoteLeader(uuid);
        return new RemoteTablet(TableId, TabletId, Partition, cache);
    }

    public RemoteTablet RemoveTabletServer(string uuid)
    {
        var cache = _cache.RemoveTabletServer(uuid);
        return new RemoteTablet(TableId, TabletId, Partition, cache);
    }

    public bool Equals(RemoteTablet? other)
    {
        if (other is null)
            return false;

        return TabletId == other.TabletId;
    }

    public override bool Equals(object? obj) => Equals(obj as RemoteTablet);

    public override int GetHashCode() => TabletId.GetHashCode();

    public override string ToString()
    {
        var leader = _cache.GetLeaderServerInfo();

        var tabletServers = _cache.Servers
            .Select(e => $"{e}{(e == leader ? "[L]" : "")}")
            // Sort so that we have a consistent iteration order.
            .OrderBy(e => e);

        return $"{TabletId}@[{string.Join(",", tabletServers)}]";
    }
}
