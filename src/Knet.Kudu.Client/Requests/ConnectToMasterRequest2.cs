namespace Knet.Kudu.Client.Requests
{
    /// <summary>
    /// Retryable version of <see cref="ConnectToMasterRequest"/>. This class
    /// avoids the special case where <see cref="ConnectToMasterRequest"/>
    /// isn't retried.
    /// </summary>
    public class ConnectToMasterRequest2 : ConnectToMasterRequest
    {
    }
}
