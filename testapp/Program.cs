using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Knet.Kudu.Client;
using Knet.Kudu.Client.Internal;
using Microsoft.Extensions.Logging;

namespace testapp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args == null || args.Length < 2)
            {
                Console.WriteLine("Usage: testapp masters should_block");
                Console.WriteLine("Example: testapp localhost:7051 true");
            }

            string masters = args[0];
            bool shouldGroup = bool.Parse(args[1]);

            TestPipe.ShouldDelay = shouldGroup;

            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddConsole();
            });

            await using var client = KuduClient.NewBuilder(masters)
                .SetLoggerFactory(loggerFactory)
                .Build();

            await client.ConnectToClusterAsync();

            Console.WriteLine("Ready to send 2 RPCs, press enter");
            Console.ReadLine();

            var tasks = new HashSet<Task>
            {
                client.GetTabletServersAsync(),
                client.GetTabletServersAsync()
            };

            if (shouldGroup)
                TestPipe.DelayTcs.SetResult(null);

            while (tasks.Count > 0)
            {
                var task = await Task.WhenAny(tasks);
                tasks.Remove(task);

                try
                {
                    await task;
                    Console.WriteLine($"RPC completed, {tasks.Count} remaining");
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("RPC timed out");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unknown RPC exception: {ex}");
                }
            }
        }
    }
}
