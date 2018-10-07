using System.Threading.Tasks;
using Kudu.Client.Negotiate;

namespace Kudu.Client.Connection
{
    // TODO: abstract this
    public class KuduConnectionFactory
    {
        public static async Task<KuduConnection> ConnectAsync(ServerInfo serverInfo)
        {
            var connection = await KuduSocketConnection.ConnectAsync(serverInfo).ConfigureAwait(false);
            var kuduConnection = new KuduConnection(connection);
            var negotiator = new Negotiator(kuduConnection);
            await negotiator.NegotiateAsync().ConfigureAwait(false);

            return kuduConnection;
        }
    }
}
