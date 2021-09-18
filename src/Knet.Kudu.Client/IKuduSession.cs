using System;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client;

/// <summary>
/// <para>
/// Within a session, multiple operations may be accumulated and batched
/// together for better efficiency. There is a guarantee that writes from
/// different sessions do not get batched together into the same RPCs --
/// this means that latency-sensitive clients can run through the same
/// <see cref="KuduClient"/> object as throughput-oriented clients, perhaps
/// by adjusting <see cref="KuduSessionOptions"/> for different uses.
/// </para>
/// 
/// <para>
/// Sessions are thread safe by default, however FlushAsync is only guaranteed
/// to flush rows for which EnqueueAsync has completed. To guarantee ordering,
/// prior calls to EnqueueAsync must be awaited before issuing another write.
/// There are no ordering guarantees across concurrent calls to EnqueueAsync.
/// Calling FlushAsync concurrently will wait for prior flushes to complete,
/// and thus will preserve ordering guarantees.
/// </para>
/// </summary>
public interface IKuduSession : IAsyncDisposable
{
    /// <summary>
    /// Asynchronously enqueues a row to the session. The row will be written
    /// to the server when one of the following:
    /// <list type="number">
    /// <item><description>
    /// <see cref="KuduSessionOptions.BatchSize"/> threshold is met
    /// </description></item>
    /// <item><description>
    /// <see cref="KuduSessionOptions.FlushInterval"/> threshold is met
    /// </description></item>
    /// <item><description>
    /// <see cref="FlushAsync(CancellationToken)"/> is called
    /// </description></item>
    /// <item><description>
    /// The session is disposed via <see cref="IAsyncDisposable.DisposeAsync"/>
    /// </description></item>
    /// </list>
    /// </summary>
    /// <param name="operation">The row to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    ValueTask EnqueueAsync(KuduOperation operation, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes all buffered rows.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task FlushAsync(CancellationToken cancellationToken = default);
}
