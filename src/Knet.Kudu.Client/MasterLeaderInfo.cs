using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client;

internal sealed record MasterLeaderInfo(
    string Location,
    string ClusterId,
    ServerInfo ServerInfo,
    HiveMetastoreConfig? HiveMetastoreConfig);
