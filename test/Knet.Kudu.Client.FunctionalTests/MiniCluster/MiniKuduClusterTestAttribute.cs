using System;
using System.Runtime.InteropServices;
using McMaster.Extensions.Xunit;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Class | AttributeTargets.Assembly, AllowMultiple = true)]
    public class MiniKuduClusterTestAttribute : Attribute, ITestCondition
    {
        private static readonly Lazy<bool> _hasKuduMiniCluster = new Lazy<bool>(HasKuduMiniCluster);

        public bool IsMet => _hasKuduMiniCluster.Value;

        public string SkipReason => "This test requires Linux with Kudu installed.";

        private static bool HasKuduMiniCluster()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                try
                {
                    // This method throws an exception if Kudu can't be found.
                    KuduBinaryLocator.FindBinaryLocation();
                    return true;
                }
                catch { }
            }

            return false;
        }
    }
}
