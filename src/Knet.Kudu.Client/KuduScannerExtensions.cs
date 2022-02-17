using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client;

public static class KuduScannerExtensions
{
    public static async ValueTask<List<T>> ScanToListAsync<T>(
        this KuduScanner scanner,
        CancellationToken cancellationToken = default)
    {
        var list = new List<T>(0);

        await foreach (var resultSet in scanner.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (list.Capacity == 0)
            {
                list.Capacity = (int)resultSet.Count;
            }

            list.AddRange(resultSet.MapTo<T>());
        }

        return list;
    }

    public static async ValueTask<long> CountAsync(
        this KuduScanner scanner,
        CancellationToken cancellationToken = default)
    {
        long count = 0;

        await foreach (var resultSet in scanner.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            count += resultSet.Count;
        }

        return count;
    }
}
