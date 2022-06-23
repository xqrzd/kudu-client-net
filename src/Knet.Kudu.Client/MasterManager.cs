using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client;

internal sealed class MasterManager
{
    private readonly object _lock = new();

    private MasterLeaderInfo? _currentLeader;
    private volatile MasterLeaderInfo? _lastKnownLeader;

    public MasterLeaderInfo? LeaderInfo
    {
        get
        {
            lock (_lock)
            {
                return _currentLeader;
            }
        }
    }

    public MasterLeaderInfo? LastKnownLeaderInfo => _lastKnownLeader;

    public void UpdateLeader(MasterLeaderInfo masterLeaderInfo)
    {
        lock (_lock)
        {
            _currentLeader = masterLeaderInfo;
        }

        _lastKnownLeader = masterLeaderInfo;
    }

    public void RemoveLeader(ServerInfo serverInfo)
    {
        lock (_lock)
        {
            var cachedServerInfo = _currentLeader?.ServerInfo;

            if (ReferenceEquals(serverInfo, cachedServerInfo))
            {
                // The bad leader is still cached, remove it.
                _currentLeader = null;
            }
        }
    }
}
