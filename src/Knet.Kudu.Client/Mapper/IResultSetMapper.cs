using System;

namespace Knet.Kudu.Client.Mapper;

public interface IResultSetMapper
{
    Func<ResultSet, int, T> CreateDelegate<T>(KuduSchema projectionSchema);
}
