using Knet.Kudu.Client.Protobuf.Master;

namespace Knet.Kudu.Client.Requests
{
    internal abstract class KuduMasterRpc<T> : KuduRpc<T>
    {
        public MasterErrorPB Error { get; protected set; }

        public KuduMasterRpc()
        {
            ServiceName = MasterServiceName;
        }
    }
}
