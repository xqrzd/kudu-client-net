using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client
{
    public enum RowOperation : byte
    {
        Insert = RowOperationsPB.Type.Insert,
        Update = RowOperationsPB.Type.Update,
        Delete = RowOperationsPB.Type.Delete,
        Upsert = RowOperationsPB.Type.Upsert,
        SplitRow = RowOperationsPB.Type.SplitRow,
        RangeLowerBound = RowOperationsPB.Type.RangeLowerBound,
        RangeUpperBound = RowOperationsPB.Type.RangeUpperBound,
        ExclusiveRangeLowerBound = RowOperationsPB.Type.ExclusiveRangeLowerBound,
        InclusiveRangeUpperBound = RowOperationsPB.Type.InclusiveRangeUpperBound,
    }
}
