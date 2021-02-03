using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Knet.Kudu.Client
{
    // TODO: Use C# 9 record class for this
    public class KuduSessionOptions
    {
        /// <summary>
        /// The maximum number of items to send in a single write request.
        /// Hitting this number of items will trigger a flush immediately,
        /// regardless of the value of <see cref="FlushInterval"/>.
        /// </summary>
        public int BatchSize { get; set; } = 1000;

        /// <summary>
        /// The maximum number of items the session may buffer.
        /// </summary>
        public int Capacity { get; set; } = 100000;

        /// <summary>
        /// True if writers to the session guarantee that there will only ever
        /// be at most one write operation at a time; false if no such constraint
        /// is guaranteed.
        /// </summary>
        public bool SingleWriter { get; set; }

        /// <summary>
        /// The maximum duration of time to wait for new items before flushing.
        /// Hitting this duration will trigger a flush immediately, regardless
        /// of the value of <see cref="BatchSize"/>.
        /// </summary>
        public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// The external consistency mode for this session.
        /// </summary>
        public ExternalConsistencyMode ExternalConsistencyMode { get; set; } =
            ExternalConsistencyMode.ClientPropagated;

        /// <summary>
        /// Optional callback to be notified of errors.
        /// </summary>
        public Func<SessionExceptionContext, ValueTask> ExceptionHandler { get; set; }
    }

    public sealed class SessionExceptionContext
    {
        public Exception Exception { get; }

        public IReadOnlyList<KuduOperation> Rows { get; }

        public SessionExceptionContext(
            Exception exception,
            IReadOnlyList<KuduOperation> rows)
        {
            Exception = exception;
            Rows = rows;
        }
    }
}
