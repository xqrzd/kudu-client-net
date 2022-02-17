using System;

namespace Knet.Kudu.Client.Mapper;

public sealed class ResultSetMapper : IResultSetMapper
{
    private readonly DelegateCache _cache = new();

    public Func<ResultSet, int, T> CreateDelegate<T>(KuduSchema projectionSchema)
    {
        if (_cache.TryGetDelegate(typeof(T), projectionSchema, out var func))
        {
            return (Func<ResultSet, int, T>)func;
        }

        return CreateNewDelegate<T>(projectionSchema);
    }

    private Func<ResultSet, int, T> CreateNewDelegate<T>(KuduSchema projectionSchema)
    {
        var func = MappingProfileFactory.Create<T>(projectionSchema);
        _cache.AddDelegate(typeof(T), projectionSchema, func);

        return func;
    }
}
