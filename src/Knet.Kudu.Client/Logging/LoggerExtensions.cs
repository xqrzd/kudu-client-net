using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Tablet;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Logging
{
    public static class LoggerExtensions
    {
        private static readonly Action<ILogger, HostAndPort, IPAddress, string, string, bool, Exception> _connectedToServer;
        private static readonly Action<ILogger, int, string, int, string, Exception> _misconfiguredMasterAddresses;
        private static readonly Action<ILogger, string, Exception> _connectionDisconnected;
        private static readonly Action<ILogger, HostAndPort, IPAddress, string, Exception> _unableToConnectToServer;
        private static readonly Action<ILogger, Exception> _exceptionSendingSessionData;
        private static readonly Action<ILogger, Exception> _exceptionClosingScanner;
        private static readonly Action<ILogger, Exception> _recoverableRpcException;
        private static readonly Action<ILogger, string, string, Exception> _scannerExpired;

        static LoggerExtensions()
        {
            _connectedToServer = LoggerMessage.Define<HostAndPort, IPAddress, string, string, bool>(
                eventId: new EventId(1, "ConnectedToServer"),
                logLevel: LogLevel.Debug,
                formatString: "Connected to {HostPort}; Ip: {Ip}; over {Tls}; with {NegotiateInfo}; IsLocal: {IsLocal}");

            _misconfiguredMasterAddresses = LoggerMessage.Define<int, string, int, string>(
                eventId: new EventId(2, "MisconfiguredMasterAddresses"),
                logLevel: LogLevel.Warning,
                formatString: "Client configured with {NumClientMasters} master(s) ({ClientMasters}) but cluster indicates it expects {NumClusterMasters} master(s) ({ClusterMasters})");

            _connectionDisconnected = LoggerMessage.Define<string>(
                eventId: new EventId(3, "ConnectionDisconnected"),
                logLevel: LogLevel.Warning,
                formatString: "Connection ungracefully closed: {Server}");

            _unableToConnectToServer = LoggerMessage.Define<HostAndPort, IPAddress, string>(
                eventId: new EventId(4, "UnableToConnectToServer"),
                logLevel: LogLevel.Warning,
                formatString: "Unable to connect to {HostPort}; Ip: {Ip}; UUID: {UUID}");

            _exceptionSendingSessionData = LoggerMessage.Define(
                eventId: new EventId(5, "ExceptionSendingSessionData"),
                logLevel: LogLevel.Error,
                formatString: "Exception occurred while flushing session data, will retry");

            _exceptionClosingScanner = LoggerMessage.Define(
                eventId: new EventId(6, "ExceptionClosingScanner"),
                logLevel: LogLevel.Warning,
                formatString: "Exception occurred while closing scanner");

            _recoverableRpcException = LoggerMessage.Define(
                eventId: new EventId(7, "RecoverableRpcException"),
                logLevel: LogLevel.Warning,
                formatString: "RPC failed, will retry");

            _scannerExpired = LoggerMessage.Define<string, string>(
                eventId: new EventId(8, "ScannerExpired"),
                logLevel: LogLevel.Warning,
                formatString: "Scanner {ScannerId} expired, creating a new one Tablet: {Tablet}");
        }

        public static void ConnectedToServer(this ILogger logger, HostAndPort hostPort, IPAddress ipAddress, string tlsInfo, string negotiateInfo, bool isLocal)
        {
            _connectedToServer(logger, hostPort, ipAddress, tlsInfo, negotiateInfo, isLocal, null);
        }

        public static void MisconfiguredMasterAddresses(this ILogger logger, IReadOnlyList<HostAndPort> clientMasters, IReadOnlyList<HostPortPB> clusterMasters)
        {
            var clientMastersStr = string.Join(",", clientMasters);
            var clusterMastersStr = string.Join(",", clusterMasters.Select(m => m.ToHostAndPort()));
            _misconfiguredMasterAddresses(logger, clientMasters.Count, clientMastersStr, clusterMasters.Count, clusterMastersStr, null);
        }

        public static void ConnectionDisconnected(this ILogger logger, string server, Exception ex)
        {
            _connectionDisconnected(logger, server, ex);
        }

        public static void UnableToConnectToServer(this ILogger logger, ServerInfo serverInfo, Exception ex)
        {
            _unableToConnectToServer(logger, serverInfo.HostPort, serverInfo.Endpoint.Address, serverInfo.Uuid, ex);
        }

        public static void ExceptionSendingSessionData(this ILogger logger, Exception ex)
        {
            _exceptionSendingSessionData(logger, ex);
        }

        public static void ExceptionClosingScanner(this ILogger logger, Exception ex)
        {
            _exceptionClosingScanner(logger, ex);
        }

        public static void RecoverableRpcException(this ILogger logger, Exception ex)
        {
            _recoverableRpcException(logger, ex);
        }

        public static void ScannerExpired(this ILogger logger, byte[] scannerId, RemoteTablet tablet)
        {
            _scannerExpired(logger, scannerId?.ToStringUtf8(), tablet.ToString(), null);
        }
    }
}
