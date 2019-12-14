using System.IO;
using Knet.Kudu.Client.Protocol.Tools;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    public class MiniKuduClusterBuilder
    {
        private readonly CreateClusterRequestPB _options;

        public MiniKuduClusterBuilder()
        {
            _options = new CreateClusterRequestPB();
        }

        public MiniKuduCluster Create()
        {
            if (string.IsNullOrWhiteSpace(_options.ClusterRoot))
            {
                _options.ClusterRoot = Path.Combine(
                    Path.GetTempPath(),
                    $"mini-kudu-cluster-{Path.GetFileNameWithoutExtension(Path.GetRandomFileName())}");
            }

            var miniCluster = new MiniKuduCluster(_options);
            miniCluster.Start();
            return miniCluster;
        }

        public MiniKuduClusterBuilder NumMasters(int numMasters)
        {
            _options.NumMasters = numMasters;
            return this;
        }

        public MiniKuduClusterBuilder NumTservers(int numTservers)
        {
            _options.NumTservers = numTservers;
            return this;
        }
    }
}
