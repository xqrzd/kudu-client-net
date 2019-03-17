using System.IO.Pipelines;
using System.Threading.Tasks;
using Kudu.Client.Negotiate;
using Pipelines.Sockets.Unofficial;

namespace Kudu.Client.Connection
{
    public class KuduConnectionFactory : IKuduConnectionFactory
    {
        private static readonly PipeOptions DefaultSendOptions = new PipeOptions(
            readerScheduler: DedicatedThreadPoolPipeScheduler.Default,
            writerScheduler: DedicatedThreadPoolPipeScheduler.Default,
            pauseWriterThreshold: 1024 * 1024 * 4,  // 4MB
            resumeWriterThreshold: 1024 * 1024 * 2, // 2MB
            minimumSegmentSize: 4096,
            useSynchronizationContext: false);

        private static readonly PipeOptions DefaultReceiveOptions = new PipeOptions(
            readerScheduler: DedicatedThreadPoolPipeScheduler.Default,
            writerScheduler: DedicatedThreadPoolPipeScheduler.Default,
            pauseWriterThreshold: 1024 * 1024 * 256,  // 256MB
            resumeWriterThreshold: 1024 * 1024 * 128, // 128MB
            minimumSegmentSize: 1024 * 128,
            useSynchronizationContext: false);

        // TODO: Allow users to supply in pipe options.

        public async Task<KuduConnection> ConnectAsync(ServerInfo serverInfo)
        {
            var socket = await KuduSocketConnection.ConnectAsync(
                serverInfo.Endpoint).ConfigureAwait(false);

            var negotiator = new Negotiator(serverInfo, socket, DefaultSendOptions, DefaultReceiveOptions);
            return await negotiator.NegotiateAsync().ConfigureAwait(false);
        }
    }
}
