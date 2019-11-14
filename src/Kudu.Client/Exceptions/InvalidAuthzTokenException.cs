namespace Kudu.Client.Exceptions
{
    /// <summary>
    /// Receiving this exception means the authorization token used to make a
    /// request is no longer valid and a new one is needed to make requests that
    /// access data.
    /// </summary>
    public class InvalidAuthzTokenException : RecoverableException
    {
        public InvalidAuthzTokenException(KuduStatus status)
            : base(status)
        {
        }
    }
}
