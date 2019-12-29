using System;
using System.Net;
using Knet.Kudu.Client.Connection;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Logging
{
    public static class LoggerExtensions
    {
        private static readonly Action<ILogger, HostAndPort, IPAddress, string, string, bool, Exception> _connectedToServer;
        private static readonly Action<ILogger, Exception> _unableToFindLeaderMaster;
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
        }

        public static void ConnectedToServer(this ILogger logger, HostAndPort hostPort, IPAddress ipAddress, string tlsInfo, string negotiateInfo, bool isLocal)
        {
            _connectedToServer(logger, hostPort, ipAddress, tlsInfo, negotiateInfo, isLocal, null);
        }

        public static void UnableToFindLeaderMaster(this ILogger logger)
        {
            _unableToFindLeaderMaster(logger, null);
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
