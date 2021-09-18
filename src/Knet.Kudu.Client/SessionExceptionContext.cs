using System;
using System.Collections.Generic;

namespace Knet.Kudu.Client;

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
