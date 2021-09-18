using System;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class TimeoutTests
{
    /// <summary>
    /// This test checks that, even if there is no event on the channel over which
    /// an RPC was sent (e.g., even if the server hangs and does not respond), RPCs
    /// will still time out.
    /// </summary>
    [SkippableFact]
    public async Task TestTimeoutEvenWhenServerHangs()
    {
        await using var harness = await new MiniKuduClusterBuilder()
            .AddTabletServerFlag("--scanner_inject_latency_on_each_batch_ms=200000")
            .BuildHarnessAsync();

        await using var client = harness.CreateClient();

        var tableBuilder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestTimeoutEvenWhenServerHangs));

        var table = await client.CreateTableAsync(tableBuilder);

        var row = ClientTestUtil.CreateBasicSchemaInsert(table, 1);
        await client.WriteAsync(new[] { row });

        var scanner = client.NewScanBuilder(table).Build();

        // Scan with a short timeout.
        var timeout = TimeSpan.FromSeconds(1);
        using var cts = new CancellationTokenSource(timeout);

        // The server will not respond for the lifetime of the test, so we
        // expect the operation to time out.
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var resultSet in scanner.WithCancellation(cts.Token))
            {
            }
        });
    }
}
