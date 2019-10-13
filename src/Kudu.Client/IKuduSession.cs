using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kudu.Client
{
    public interface IKuduSession : IAsyncDisposable
    {
        ValueTask EnqueueAsync(Operation operation, CancellationToken cancellationToken = default);

        Task FlushAsync(CancellationToken cancellationToken = default);
    }
}
