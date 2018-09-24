using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Tasks;
using Pipelines.Sockets.Unofficial;

namespace Kudu.Client.Connection
{
    public class KuduSocketConnection : IConnection
    {
        private const byte CurrentRpcVersion = 9;

        private static readonly byte[] ConnectionHeader = new byte[]
        {
            (byte)'h', (byte)'r', (byte)'p', (byte)'c',
            CurrentRpcVersion,
            0,
            0
        };

        private readonly SocketConnection _connection;

        public PipeReader Input => _connection.Input;

        public PipeWriter Output => _connection.Output;

        public KuduSocketConnection(SocketConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public override string ToString() => _connection.ToString();

        public void Dispose()
        {
            _connection.Dispose();
        }

        public static async Task<KuduSocketConnection> ConnectAsync(HostAndPort hostPort)
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            SocketConnection.SetRecommendedClientOptions(socket);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

            await socket.ConnectAsync(hostPort.Host, hostPort.Port);
            var connection = SocketConnection.Create(socket, name: hostPort.ToString());

            // After the client connects to a server, the client first sends a connection header.
            // The connection header consists of a magic number "hrpc" and three byte flags, for a total of 7 bytes.
            // https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#wire-protocol
            // TODO: Should this be done elsewhere?
            await connection.Output.WriteAsync(ConnectionHeader);

            return new KuduSocketConnection(connection);
        }
    }
}
