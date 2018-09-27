using System.Collections.Generic;
using Kudu.Client.Connection;

namespace Kudu.Client
{
    public class KuduClientSettings
    {
        public List<HostAndPort> MasterAddresses { get; set; }
    }
}
