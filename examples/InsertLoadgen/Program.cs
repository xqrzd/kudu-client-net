using System.Threading.Tasks;
using Knet.Kudu.Client;
using Microsoft.Extensions.Logging;

namespace InsertLoadgen
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddConsole();
            });

            await using var client = KuduClient.NewBuilder("localhost:7051")
                .SetLoggerFactory(loggerFactory)
                .Build();
        }
    }
}
