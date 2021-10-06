using System;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client;

public record KuduSessionOptions
{
    /// <summary>
    /// The maximum number of items to send in a single write request.
    /// Hitting this number of items will trigger a flush immediately,
    /// regardless of the value of <see cref="FlushInterval"/>.
    /// Default: 2000.
    /// </summary>
    public int BatchSize { get; init; } = 2000;

    /// <summary>
    /// The maximum number of items the session may store before calls to
    /// <see cref="IKuduSession.EnqueueAsync(KuduOperation, CancellationToken)"/>
    /// will block until a batch completes. Default: 40000.
    /// </summary>
    public int Capacity { get; init; } = 40000;

    /// <summary>
    /// The maximum duration of time to wait for new items before flushing.
    /// Hitting this duration will trigger a flush immediately, regardless
    /// of the value of <see cref="BatchSize"/>. Default: 1 second.
    /// </summary>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The external consistency mode for this session.
    /// Default: <see cref="ExternalConsistencyMode.ClientPropagated"/>.
    /// </summary>
    public ExternalConsistencyMode ExternalConsistencyMode { get; init; } =
        ExternalConsistencyMode.ClientPropagated;

    /// <summary>
    /// Optional callback to be notified of errors.
    /// </summary>
    public Func<SessionExceptionContext, ValueTask>? ExceptionHandler { get; init; }
}
