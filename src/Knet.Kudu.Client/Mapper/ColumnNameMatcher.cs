using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;

namespace Knet.Kudu.Client.Mapper;

internal sealed class ColumnNameMatcher<T> where T : class
{
#if NETSTANDARD2_0
    private static readonly StringComparer _columnComparer = StringComparer.Create(
        CultureInfo.InvariantCulture,
        ignoreCase: true);
#else
    private static readonly StringComparer _columnComparer = StringComparer.Create(
        CultureInfo.InvariantCulture,
        CompareOptions.IgnoreCase | CompareOptions.IgnoreNonSpace | CompareOptions.IgnoreSymbols);
#endif

    private readonly ILookup<string, T> _projectedColumns;
    private readonly Func<T, string> _nameSelector;

    public ColumnNameMatcher(IEnumerable<T> projectedColumns, Func<T, string> nameSelector)
    {
        _projectedColumns = projectedColumns.ToLookup(nameSelector, _columnComparer);
        _nameSelector = nameSelector;
    }

    public bool TryGetColumn(string destinationName, [NotNullWhen(true)] out T? columnInfo)
    {
        var columns = _projectedColumns[destinationName];

        T? caseInsensitiveMatch = null;
        T? firstMatch = null;

        // We could get multiple matches here, take the best one.
        foreach (var column in columns)
        {
            var projectedName = _nameSelector(column);

            if (StringComparer.Ordinal.Equals(destinationName, projectedName))
            {
                // Exact match.
                columnInfo = column;
                return true;
            }

            if (StringComparer.OrdinalIgnoreCase.Equals(destinationName, projectedName))
            {
                if (caseInsensitiveMatch is null)
                    caseInsensitiveMatch = column;
            }
            else
            {
                if (firstMatch is null)
                    firstMatch = column;
            }
        }

        if (caseInsensitiveMatch is not null)
        {
            columnInfo = caseInsensitiveMatch;
            return true;
        }

        if (firstMatch is not null)
        {
            columnInfo = firstMatch;
            return true;
        }

        columnInfo = null;
        return false;
    }
}
