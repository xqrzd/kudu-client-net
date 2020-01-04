using System;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client
{
    public interface IKuduSession : IAsyncDisposable
    {
        ValueTask EnqueueAsync(KuduOperation operation, CancellationToken cancellationToken = default);

        Task FlushAsync(CancellationToken cancellationToken = default);
    }
}
