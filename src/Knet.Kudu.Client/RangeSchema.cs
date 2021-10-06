using System.Collections.Generic;

namespace Knet.Kudu.Client;

public class RangeSchema
{
    public List<int> ColumnIds { get; }

    public RangeSchema(List<int> columnIds)
    {
        ColumnIds = columnIds;
    }
}
