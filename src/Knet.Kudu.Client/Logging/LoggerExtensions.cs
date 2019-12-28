using System;
using System.Net;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Master;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client.Logging
{
    public static class LoggerExtensions
    {
        private static readonly Action<ILogger, Exception> _exceptionConnectingToMaster;
        private static readonly Action<ILogger, MasterErrorPB.Code, AppStatusPB.ErrorCode, string, Exception> _connectToMasterFailed;
        private static readonly Action<ILogger, HostAndPort, IPAddress, string, string, bool, Exception> _connectedToServer;
        private static readonly Action<ILogger, Exception> _exceptionSendingSessionData;
        private static readonly Action<ILogger, string, Exception> _connectionDisconnected;

        static LoggerExtensions()
        {
            _exceptionConnectingToMaster = LoggerMessage.Define(
                eventId: new EventId(1, "ExceptionConnectingToMaster"),
                logLevel: LogLevel.Warning,
                formatString: "Exception occurred while connecting to a master server");

            _connectToMasterFailed = LoggerMessage.Define<MasterErrorPB.Code, AppStatusPB.ErrorCode, string>(
                eventId: new EventId(2, "ConnectToMasterFailed"),
                logLevel: LogLevel.Warning,
                formatString: "ConnectToMaster RPC returned an error with MasterCode: {MasterCode}; and App-Code: {AppCode}; and Message: {Message}");

            _connectedToServer = LoggerMessage.Define<HostAndPort, IPAddress, string, string, bool>(
                eventId: new EventId(3, "ConnectedToServer"),
                logLevel: LogLevel.Debug,
                formatString: "Connected to {HostPort}; Ip: {Ip}; over {Tls}; with {NegotiateInfo}; IsLocal: {IsLocal}");

            _exceptionConnectingToMaster = LoggerMessage.Define(
                eventId: new EventId(4, "ExceptionConnectingToMaster"),
                logLevel: LogLevel.Warning,
                formatString: "Exception occurred while connecting to a master server");

            _exceptionSendingSessionData = LoggerMessage.Define(
                eventId: new EventId(5, "ExceptionSendingSessionData"),
                logLevel: LogLevel.Error,
                formatString: "Exception occurred while flushing session data, will retry");

            _connectionDisconnected = LoggerMessage.Define<string>(
                eventId: new EventId(6, "ConnectionDisconnected"),
                logLevel: LogLevel.Warning,
                formatString: "Connection ungracefully closed: {Server}");
        }

        public static void ExceptionConnectingToMaster(this ILogger logger, Exception ex)
        {
            _exceptionConnectingToMaster(logger, ex);
        }

        public static void ConnectToMasterFailed(this ILogger logger, MasterErrorPB error)
        {
            _connectToMasterFailed(logger, error.code, error.Status.Code, error.Status.Message, null);
        }

        public static void ConnectedToServer(this ILogger logger, HostAndPort hostPort, IPAddress ipAddress, string tlsInfo, string negotiateInfo, bool isLocal)
        {
            _connectedToServer(logger, hostPort, ipAddress, tlsInfo, negotiateInfo, isLocal, null);
        }

        public static void ExceptionSendingSessionData(this ILogger logger, Exception ex)
        {
            _exceptionSendingSessionData(logger, ex);
        }

        public static void ConnectionDisconnected(this ILogger logger, string server, Exception ex)
        {
            _connectionDisconnected(logger, server, ex);
        }
    }
}
