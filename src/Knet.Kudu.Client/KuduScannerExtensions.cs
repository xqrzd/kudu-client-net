using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client;

public static class KuduScannerExtensions
{
    /// <summary>
    /// Enumerates the scanner and maps the results to the generic type.
    /// </summary>
    /// <typeparam name="T">The type to project a row to.</typeparam>
    public static async ValueTask<List<T>> ScanToListAsync<T>(
        this KuduScanner scanner,
        CancellationToken cancellationToken = default)
    {
        var list = new List<T>(0);

        await foreach (var resultSet in scanner.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            resultSet.MapTo(list);
        }

        return list;
    }

    /// <summary>
    /// Counts the number of rows returned by the scanner. Use
    /// <see cref="AbstractKuduScannerBuilder{TBuilder}.SetEmptyProjection"/>
    /// when constructing the scanner to avoid transferring unnecessary data.
    /// </summary>
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
