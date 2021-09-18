using System.IO;
using System.Threading.Tasks;
using Knet.Kudu.Client.Protobuf.Tools;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster;

public class MiniKuduClusterBuilder
{
    private readonly CreateClusterRequestPB _options;

    public MiniKuduClusterBuilder()
    {
        _options = new CreateClusterRequestPB
        {
            NumMasters = 3,
            NumTservers = 3
        };
    }

    /// <summary>
    /// Builds and starts a new <see cref="MiniKuduCluster"/>.
    /// </summary>
    public async Task<MiniKuduCluster> BuildAsync()
    {
        if (string.IsNullOrWhiteSpace(_options.ClusterRoot))
        {
            _options.ClusterRoot = Path.Combine(
                Path.GetTempPath(),
                $"mini-kudu-cluster-{Path.GetFileNameWithoutExtension(Path.GetRandomFileName())}");
        }

        var miniCluster = new MiniKuduCluster(_options);
        await miniCluster.StartAsync();
        return miniCluster;
    }

    public async Task<KuduTestHarness> BuildHarnessAsync()
    {
        var miniCluster = await BuildAsync();
        return new KuduTestHarness(miniCluster, disposeMiniCluster: true);
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

    /// <summary>
    /// Adds a new flag to be passed to the Master daemons on start.
    /// </summary>
    /// <param name="flag">The flag to pass.</param>
    public MiniKuduClusterBuilder AddMasterServerFlag(string flag)
    {
        _options.ExtraMasterFlags.Add(flag);
        return this;
    }

    /// <summary>
    /// Adds a new flag to be passed to the Tablet Server daemons on start.
    /// </summary>
    /// <param name="flag">The flag to pass.</param>
    public MiniKuduClusterBuilder AddTabletServerFlag(string flag)
    {
        _options.ExtraTserverFlags.Add(flag);
        return this;
    }
}
