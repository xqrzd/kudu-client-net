using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class SessionTests : IAsyncLifetime
{
    private KuduTestHarness _harness;
    private KuduClient _client;

    public async Task InitializeAsync()
    {
        _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
        _client = _harness.CreateClient();
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _harness.DisposeAsync();
    }

    [SkippableFact]
    public async Task TestExceptionCallback()
    {
        int numCallbacks = 0;
        SessionExceptionContext sessionContext = null;

        var builder = ClientTestUtil.GetBasicSchema()
            .SetTableName(nameof(TestExceptionCallback));

        var table = await _client.CreateTableAsync(builder);
        var row1 = ClientTestUtil.CreateBasicSchemaInsert(table, 1);
        var row2 = ClientTestUtil.CreateBasicSchemaInsert(table, 1);

        var sessionOptions = new KuduSessionOptions
        {
            ExceptionHandler = HandleSessionExceptionAsync
        };

        await using var session = _client.NewSession(sessionOptions);

        await session.EnqueueAsync(row1);
        await session.FlushAsync();

        await session.EnqueueAsync(row2);
        await session.FlushAsync();

        ValueTask HandleSessionExceptionAsync(SessionExceptionContext context)
        {
            numCallbacks++;
            sessionContext = context;
            return new ValueTask();
        }

        Assert.Equal(1, numCallbacks);

        var errorRow = Assert.Single(sessionContext.Rows);
        Assert.Same(row2, errorRow);

        var exception = Assert.IsType<KuduWriteException>(sessionContext.Exception);
        var exceptionRow = Assert.Single(exception.PerRowErrors);
        Assert.True(exceptionRow.IsAlreadyPresent);
    }
}
