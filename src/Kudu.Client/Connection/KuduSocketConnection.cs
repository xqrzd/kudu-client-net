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

        private SocketConnection _connection;

        public HostAndPort ServerInfo { get; }

        public PipeReader Input => _connection.Input;

        public PipeWriter Output => _connection.Output;

        public KuduSocketConnection(HostAndPort serverInfo)
        {
            ServerInfo = serverInfo;
        }

        public async Task ConnectAsync()
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            SocketConnection.SetRecommendedClientOptions(socket);

            await socket.ConnectAsync(ServerInfo.Host, ServerInfo.Port);
            _connection = SocketConnection.Create(socket);

            // https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#wire-protocol
            // After the client connects to a server, the client first sends a connection header.
            // The connection header consists of a magic number "hrpc" and three byte flags, for a total of 7 bytes.
            // TODO: Move this elsewhere.
            await _connection.Output.WriteAsync(ConnectionHeader);
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
