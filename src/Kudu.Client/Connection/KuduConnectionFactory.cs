using System.Threading.Tasks;
using Kudu.Client.Negotiate;

namespace Kudu.Client.Connection
{
    // TODO: abstract this
    public class KuduConnectionFactory
    {
        public static async Task<KuduConnection> ConnectAsync(HostAndPort hostPort)
        {
            var connection = await KuduSocketConnection.ConnectAsync(hostPort);
            var kuduConnection = new KuduConnection(connection);
            var negotiator = new Negotiator(kuduConnection);
            await negotiator.NegotiateAsync();

            return kuduConnection;
        }
    }
}
