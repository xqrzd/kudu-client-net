namespace Kudu.Client.Requests
{
    public abstract class KuduMasterRpc<TRequest, TResponse> : KuduRpc<TRequest, TResponse>
    {
        public override string ServiceName => MasterServiceName;
    }
}
