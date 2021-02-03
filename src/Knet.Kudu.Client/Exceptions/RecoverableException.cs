using System;

namespace Knet.Kudu.Client.Exceptions
{
    /// <summary>
    /// An exception that's possible to retry.
    /// </summary>
    public class RecoverableException : KuduException
    {
        public RecoverableException(KuduStatus status)
            : base(status)
        {
        }

        public RecoverableException(KuduStatus status, Exception innerException)
            : base(status, innerException)
        {
        }
    }
}
