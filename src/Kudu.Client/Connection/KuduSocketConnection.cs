using System;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Pipelines.Sockets.Unofficial;

namespace Kudu.Client.Connection
{
    public class KuduSocketConnection : IDuplexPipe, IDisposable
    {
        private const byte CurrentRpcVersion = 9;

        private static readonly byte[] ConnectionHeader = new byte[]
        {
            (byte)'h', (byte)'r', (byte)'p', (byte)'c',
            CurrentRpcVersion,
            0, // ServiceClass (unused)
            0  // AuthProtocol (unused)
        };

        private readonly SocketConnection _connection;

        public KuduSocketConnection(SocketConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public PipeReader Input => _connection.Input;

        public PipeWriter Output => _connection.Output;

        public override string ToString() => _connection.ToString();

        public void Dispose()
        {
            _connection.Dispose();
        }

        public static async Task<KuduSocketConnection> ConnectAsync(ServerInfo serverInfo)
        {
            var connection = await SocketConnection.ConnectAsync(
                serverInfo.Endpoint,
                name: serverInfo.ToString()).ConfigureAwait(false);

            // After the client connects to a server, the client first sends a connection header.
            // The connection header consists of a magic number "hrpc" and three byte flags, for a total of 7 bytes.
            // https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#wire-protocol
            // TODO: Should this be done elsewhere?
            await connection.Output.WriteAsync(ConnectionHeader).ConfigureAwait(false);

            return new KuduSocketConnection(connection);
        }
    }
}
