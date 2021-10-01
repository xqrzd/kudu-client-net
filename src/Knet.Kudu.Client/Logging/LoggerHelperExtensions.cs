using System;
using System.Collections.Generic;
using System.Linq;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Tablet;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Logging;

internal static class LoggerHelperExtensions
{
    public static void MisconfiguredMasterAddresses(
        this ILogger logger,
        IReadOnlyList<HostAndPort> clientMasters,
        IReadOnlyList<HostPortPB> clusterMasters)
    {
        var clientMastersStr = string.Join(",", clientMasters);
        var clusterMastersStr = string.Join(",", clusterMasters
            .Select(m => m.ToHostAndPort()));

        logger.MisconfiguredMasterAddresses(
            clientMasters.Count,
            clusterMasters.Count,
            clientMastersStr,
            clusterMastersStr);
    }

    public static void UnableToConnectToServer(
        this ILogger logger,
        Exception exception,
        ServerInfo serverInfo)
    {
        var hostPort = serverInfo.HostPort;
        var ip = serverInfo.Endpoint.Address;
        var uuid = serverInfo.Uuid;

        logger.UnableToConnectToServer(exception, hostPort, ip, uuid);
    }

    public static void ScannerExpired(
        this ILogger logger,
        byte[]? scannerId,
        string tableName,
        RemoteTablet? tablet)
    {
        var scannerIdStr = scannerId?.ToStringUtf8();
        var tabletId = tablet?.TabletId;

        logger.ScannerExpired(scannerIdStr, tableName, tabletId);
    }
}
