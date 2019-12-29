﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Logging
{
    public static class LoggerExtensions
    {
        private static readonly Action<ILogger, HostAndPort, IPAddress, string, string, bool, Exception> _connectedToServer;
        private static readonly Action<ILogger, Exception> _unableToFindLeaderMaster;
        private static readonly Action<ILogger, int, string, int, string, Exception> _misconfiguredMasterAddresses;
        private static readonly Action<ILogger, string, Exception> _connectionDisconnected;
        private static readonly Action<ILogger, HostAndPort, IPAddress, string, Exception> _unableToConnectToServer;
        private static readonly Action<ILogger, Exception> _exceptionSendingSessionData;

        static LoggerExtensions()
        {
            _connectedToServer = LoggerMessage.Define<HostAndPort, IPAddress, string, string, bool>(
                eventId: new EventId(1, "ConnectedToServer"),
                logLevel: LogLevel.Debug,
                formatString: "Connected to {HostPort}; Ip: {Ip}; over {Tls}; with {NegotiateInfo}; IsLocal: {IsLocal}");

            _unableToFindLeaderMaster = LoggerMessage.Define(
                eventId: new EventId(2, "UnableToFindLeaderMaster"),
                logLevel: LogLevel.Warning,
                formatString: "Unable to find a leader master");

            _misconfiguredMasterAddresses = LoggerMessage.Define<int, string, int, string>(
                eventId: new EventId(3, "MisconfiguredMasterAddresses"),
                logLevel: LogLevel.Warning,
                formatString: "Client configured with {NumClientMasters} master(s) ({ClientMasters}) but cluster indicates it expects {NumClusterMasters} master(s) ({ClusterMasters})");

            _connectionDisconnected = LoggerMessage.Define<string>(
                eventId: new EventId(4, "ConnectionDisconnected"),
                logLevel: LogLevel.Warning,
                formatString: "Connection ungracefully closed: {Server}");

            _unableToConnectToServer = LoggerMessage.Define<HostAndPort, IPAddress, string>(
                eventId: new EventId(5, "UnableToConnectToServer"),
                logLevel: LogLevel.Warning,
                formatString: "Unable to connect to {HostPort}; Ip: {Ip}; UUID: {UUID}");

            _exceptionSendingSessionData = LoggerMessage.Define(
                eventId: new EventId(6, "ExceptionSendingSessionData"),
                logLevel: LogLevel.Error,
                formatString: "Exception occurred while flushing session data, will retry");
        }

        public static void ConnectedToServer(this ILogger logger, HostAndPort hostPort, IPAddress ipAddress, string tlsInfo, string negotiateInfo, bool isLocal)
        {
            _connectedToServer(logger, hostPort, ipAddress, tlsInfo, negotiateInfo, isLocal, null);
        }

        public static void UnableToFindLeaderMaster(this ILogger logger)
        {
            _unableToFindLeaderMaster(logger, null);
        }

        public static void MisconfiguredMasterAddresses(this ILogger logger, IReadOnlyList<HostAndPort> clientMasters, List<HostPortPB> clusterMasters)
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
    }
}
