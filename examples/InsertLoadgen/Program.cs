using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Knet.Kudu.Client;
using Microsoft.Extensions.Logging;

namespace InsertLoadgen;

class Program
{
    static async Task Main()
    {
        var masterAddresses = "localhost:7051,localhost:7151,localhost:7251";
        var tableName = $"test_table_{Guid.NewGuid():N}";
        var numRows = 10000;

        var loggerFactory = LoggerFactory.Create(builder => builder
            .SetMinimumLevel(LogLevel.Trace)
            .AddConsole());

        await using var client = KuduClient.NewBuilder(masterAddresses)
            .SetLoggerFactory(loggerFactory)
            .Build();

        var tableBuilder = new TableBuilder(tableName)
            .AddColumn("host", KuduType.String, opt => opt.Key(true))
            .AddColumn("metric", KuduType.String, opt => opt.Key(true))
            .AddColumn("timestamp", KuduType.UnixtimeMicros, opt => opt.Key(true))
            .AddColumn("value", KuduType.Double)
            .SetNumReplicas(1)
            .SetRangePartitionColumns("host", "metric", "timestamp");

        var table = await client.CreateTableAsync(tableBuilder);
        Console.WriteLine($"Created table {tableName}");

        var batches = CreateRows(table, numRows).Chunk(2000);
        var writtenRows = 0;

        foreach (var batch in batches)
        {
            await client.WriteAsync(batch);
            writtenRows += batch.Length;
            Console.WriteLine($"Wrote {writtenRows} rows");
        }
    }

    private static IEnumerable<KuduOperation> CreateRows(KuduTable table, int numRows)
    {
        var hosts = new[] { "host1.example.com", "host2.example.com" };
        var metrics = new[] { "cpuload.avg1", "cpuload.avg5", "cpuload.avg15" };
        var now = DateTime.UtcNow;

        for (int i = 0; i < numRows; i++)
        {
            var row = table.NewInsert();
            row.SetString("host", hosts[i % hosts.Length]);
            row.SetString("metric", metrics[i % metrics.Length]);
            row.SetDateTime("timestamp", now.AddSeconds(i));
            row.SetDouble("value", Random.Shared.NextDouble());

            yield return row;
        }
    }
}
