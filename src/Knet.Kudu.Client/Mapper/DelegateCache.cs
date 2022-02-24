using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Knet.Kudu.Client.Mapper;

internal sealed class DelegateCache
{
    private readonly ConcurrentDictionary<CacheKey, Delegate> _cache = new(new CacheKeyComparer());

    public bool TryGetDelegate(
        Type destinationType,
        KuduSchema projectionSchema,
        [NotNullWhen(true)] out Delegate? value)
    {
        var key = new CacheKey(destinationType, projectionSchema);
        return _cache.TryGetValue(key, out value);
    }

    public void AddDelegate(
        Type destinationType,
        KuduSchema projectionSchema,
        Delegate value)
    {
        var key = new CacheKey(destinationType, projectionSchema);
        _cache[key] = value;
    }

    private readonly record struct CacheKey(Type DestinationType, KuduSchema Schema);

    private sealed class CacheKeyComparer : IEqualityComparer<CacheKey>
    {
        public bool Equals(CacheKey x, CacheKey y)
        {
            if (x.DestinationType != y.DestinationType)
                return false;

            var columnsX = x.Schema.Columns;
            var columnsY = y.Schema.Columns;

            if (columnsX.Count != columnsY.Count)
                return false;

            var numColumns = columnsX.Count;

            for (int i = 0; i < numColumns; i++)
            {
                var columnX = columnsX[i];
                var columnY = columnsY[i];

                if (columnX != columnY)
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode([DisallowNull] CacheKey obj)
        {
            var hashcode = new HashCode();
            hashcode.Add(obj.DestinationType);

            foreach (var column in obj.Schema.Columns)
            {
                hashcode.Add(column);
            }

            return hashcode.ToHashCode();
        }
    }
}
