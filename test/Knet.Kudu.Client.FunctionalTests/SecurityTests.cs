using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class SecurityTests
{
    [SkippableFact]
    public async Task TestKuduRequireAuthenticationInsecureCluster()
    {
        await using var harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
        await using var client = harness.CreateClientBuilder()
            .RequireAuthentication(true)
            .Build();

        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestKuduRequireAuthenticationInsecureCluster));

        var exception = await Assert.ThrowsAsync<NonRecoverableException>(
            () => client.CreateTableAsync(builder));

        Assert.Contains(
            "Client requires authentication, but server does not have Kerberos enabled",
            exception.Message);
    }

    [SkippableFact]
    public async Task TestKuduRequireEncryptionInsecureCluster()
    {
        await using var harness = await new MiniKuduClusterBuilder()
            .AddMasterServerFlag("--rpc_encryption=disabled")
            .AddMasterServerFlag("--rpc_authentication=disabled")
            .AddTabletServerFlag("--rpc_encryption=disabled")
            .AddTabletServerFlag("--rpc_authentication=disabled")
            .BuildHarnessAsync();

        await using var client = harness.CreateClientBuilder()
            .SetEncryptionPolicy(EncryptionPolicy.RequiredRemote)
            .Build();

        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestKuduRequireEncryptionInsecureCluster));

        var exception = await Assert.ThrowsAsync<NonRecoverableException>(
            () => client.CreateTableAsync(builder));

        Assert.Contains(
            "Server does not support required TLS encryption",
            exception.Message);
    }

    [SkippableFact]
    public async Task TestKuduRequireEncryption()
    {
        await using var harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
        await using var client = harness.CreateClientBuilder()
            .SetEncryptionPolicy(EncryptionPolicy.Required)
            .Build();

        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestKuduRequireEncryption));

        var table = await client.CreateTableAsync(builder);
        Assert.NotNull(table.TableId);
    }

    [SkippableFact]
    public async Task TestKuduOptionalEncryption()
    {
        await using var harness = await new MiniKuduClusterBuilder()
            .AddMasterServerFlag("--rpc_encryption=disabled")
            .AddMasterServerFlag("--rpc_authentication=disabled")
            .AddTabletServerFlag("--rpc_encryption=disabled")
            .AddTabletServerFlag("--rpc_authentication=disabled")
            .BuildHarnessAsync();

        await using var client = harness.CreateClientBuilder()
            .SetEncryptionPolicy(EncryptionPolicy.Optional)
            .Build();

        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestKuduOptionalEncryption));

        var table = await client.CreateTableAsync(builder);
        Assert.NotNull(table.TableId);
    }
}
