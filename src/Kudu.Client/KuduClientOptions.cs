using System.Collections.Generic;
using System.IO.Pipelines;
using Kudu.Client.Connection;

namespace Kudu.Client
{
    public class KuduClientOptions
    {
        public List<HostAndPort> MasterAddresses { get; set; }

        public PipeOptions SendPipeOptions { get; set; }

        public PipeOptions ReceivePipeOptions { get; set; }
    }
}
