using System;
using System.Collections.Generic;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client;

public class TabletServerInfo
{
    /// <summary>
    /// Unique ID which is created when the server is first started
    /// up. This is stored persistently on disk.
    /// </summary>
    public string TsUuid { get; }

    public int MillisSinceHeartbeat { get; }

    public string Location { get; }

    public TabletServerState State { get; }

    public IReadOnlyList<HostAndPort> RpcAddresses { get; }

    public IReadOnlyList<HostAndPort> HttpAddresses { get; }

    public string SoftwareVersion { get; }

    /// <summary>
    /// True if HTTPS has been enabled for the web interface.
    /// In this case, https:// URLs should be generated for the above
    /// 'http_addresses' field.
    /// </summary>
    public bool HttpsEnabled { get; }

    /// <summary>
    /// The wall clock time when the server started.
    /// </summary>
    public DateTimeOffset StartTime { get; }

    public TabletServerInfo(
        string tsUuid,
        int millisSinceHeartbeat,
        string location,
        TabletServerState state,
        IReadOnlyList<HostAndPort> rpcAddresses,
        IReadOnlyList<HostAndPort> httpAddresses,
        string softwareVersion,
        bool httpsEnabled,
        DateTimeOffset startTime)
    {
        TsUuid = tsUuid;
        MillisSinceHeartbeat = millisSinceHeartbeat;
        Location = location;
        State = state;
        RpcAddresses = rpcAddresses;
        HttpAddresses = httpAddresses;
        SoftwareVersion = softwareVersion;
        HttpsEnabled = httpsEnabled;
        StartTime = startTime;
    }
}

public enum TabletServerState
{
    /// <summary>
    /// Default value for backwards compatibility.
    /// </summary>
    Unknown = TServerStatePB.UnknownState,
    /// <summary>
    /// No state for the tserver.
    /// </summary>
    None = TServerStatePB.None,
    /// <summary>
    /// New replicas are not added to the tserver, and failed replicas on
    /// the tserver are not re-replicated.
    /// </summary>
    MaintenanceMode = TServerStatePB.MaintenanceMode,
}
