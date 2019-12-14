using System;

namespace Knet.Kudu.Client.Exceptions
{
    /// <summary>
    /// Indicates that the request failed because we couldn't find a leader.
    /// It is retried as long as the original call hasn't timed out.
    /// </summary>
    public class NoLeaderFoundException : RecoverableException
    {
        public NoLeaderFoundException(KuduStatus status)
            : base(status)
        {
        }

        public NoLeaderFoundException(KuduStatus status, Exception innerException)
            : base(status, innerException)
        {
        }
    }
}
