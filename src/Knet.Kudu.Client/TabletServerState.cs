using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client;

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
