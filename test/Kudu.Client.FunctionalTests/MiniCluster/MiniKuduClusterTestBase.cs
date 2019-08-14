using System;

namespace Kudu.Client.FunctionalTests.MiniCluster
{
    [MiniKuduClusterTest]
    public abstract class MiniKuduClusterTestBase : IDisposable
    {
        protected MiniKuduCluster MiniKuduCluster;
        protected KuduClient Client;

        public void Dispose()
        {
            Client?.DisposeAsync().GetAwaiter().GetResult();
            MiniKuduCluster?.Dispose();
        }

        protected KuduClient GetKuduClient()
        {
            if (Client != null)
                return Client;

            MiniKuduCluster = new MiniKuduCluster();
            MiniKuduCluster.Start();
            Client = MiniKuduCluster.GetKuduClient();
            return Client;
        }
    }
}
