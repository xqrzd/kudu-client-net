using System;
using System.Net;
using Knet.Kudu.Client.Connection;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Logging;

internal static partial class LoggerExtensions
{
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        EventName = "ConnectedToServer",
        Message = "Connected to {HostPort}, Ip: {Ip}, Encryption: {Encryption}, " +
                  "Authentication: {Authentication}")]
    public static partial void ConnectedToServer(
        this ILogger logger,
        HostAndPort HostPort,
        IPAddress Ip,
        string Encryption,
        string Authentication,
        string? TlsCipher,
        string? ServicePrincipalName,
        bool IsLocal);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Warning,
        EventName = "MisconfiguredMasterAddresses",
        Message = "Client configured with {NumClientMasters} master(s) but cluster " +
                  "indicates it expects {NumClusterMasters} master(s)")]
    public static partial void MisconfiguredMasterAddresses(
        this ILogger logger,
        int NumClientMasters,
        int NumClusterMasters,
        string ClientMasters,
        string ClusterMasters);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Warning,
        EventName = "ConnectionDisconnected",
        Message = "Connection to {Server} closed ungracefully")]
    public static partial void ConnectionDisconnected(
        this ILogger logger,
        Exception exception,
        string Server);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Warning,
        EventName = "UnableToConnectToServer",
        Message = "Unable to connect to {HostPort}, Ip: {Ip}, UUID: {Uuid}")]
    public static partial void UnableToConnectToServer(
        this ILogger logger,
        Exception exception,
        HostAndPort HostPort,
        IPAddress Ip,
        string Uuid);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Error,
        EventName = "ExceptionFlushingSessionData",
        Message = "Session failed to flush {NumRows} rows")]
    public static partial void ExceptionFlushingSessionData(
        this ILogger logger,
        Exception exception,
        int NumRows,
        long TxnId);

    [LoggerMessage(
        EventId = 6,
        Level = LogLevel.Warning,
        EventName = "RecoverableRpcException",
        Message = "RPC attempt {Attempt} failed, will retry")]
    public static partial void RecoverableRpcException(
        this ILogger logger,
        Exception exception,
        int Attempt);

    [LoggerMessage(
        EventId = 7,
        Level = LogLevel.Warning,
        EventName = "ScannerExpired",
        Message = "Scanner {ScannerId} for {Table} expired, creating a new one")]
    public static partial void ScannerExpired(
        this ILogger logger,
        string? ScannerId,
        string Table,
        string? TabletId);

    [LoggerMessage(
        EventId = 8,
        Level = LogLevel.Warning,
        EventName = "ReceivedUnknownRpc",
        Message = "Received unknown RPC call ID {CallId} from {Server}")]
    public static partial void ReceivedUnknownRpc(
        this ILogger logger,
        int CallId,
        string Server);

    [LoggerMessage(
        EventId = 9,
        Level = LogLevel.Information,
        EventName = "TransactionKeepaliveTerminated",
        Message = "Transaction keepalive terminated for TxnId {TxnId}")]
    public static partial void TransactionKeepaliveTerminated(
        this ILogger logger,
        Exception exception,
        long TxnId,
        TimeSpan KeepaliveInterval);

    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Warning,
        EventName = "SetTcpKeepAliveFailed",
        Message = "Unable to set TCP keepalive for connection")]
    public static partial void SetTcpKeepAliveFailed(
        this ILogger logger,
        Exception exception);
}
