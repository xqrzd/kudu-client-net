using System;

namespace Kudu.Client.Exceptions
{
    /// <summary>
    /// An attempt to use a closed connection. This is a transient error,
    /// as an RPC can be retried when a new connection is opened.
    /// </summary>
    public class ConnectionClosedException : Exception
    {
        public bool GracefulClose => InnerException == null;

        public ConnectionClosedException(string connection, Exception innerException = null)
            : base(GetErrorMessage(connection, innerException), innerException)
        {
        }

        public static string GetErrorMessage(string connection, Exception innerException)
        {
            if (innerException == null)
                return $"KuduConnection {connection} is closed.";
            else
                return $"KuduConnection {connection} closed ungracefully. Reason: {innerException.Message}";
        }
    }
}
