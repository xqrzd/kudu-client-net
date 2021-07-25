using static Knet.Kudu.Client.Protobuf.RowOperationsPB.Types;

namespace Knet.Kudu.Client
{
    /// <summary>
    /// A set of operations (INSERT, UPDATE, UPSERT, or DELETE) to apply to a table,
    /// or the set of split rows and range bounds when creating or altering table.
    /// Range bounds determine the boundaries of range partitions during table
    /// creation, split rows further subdivide the ranges into more partitions.
    /// </summary>
    public enum RowOperation : byte
    {
        Insert = Type.Insert,
        Update = Type.Update,
        Delete = Type.Delete,
        Upsert = Type.Upsert,
        InsertIgnore = Type.InsertIgnore,
        UpdateIgnore = Type.UpdateIgnore,
        DeleteIgnore = Type.DeleteIgnore,
        /// <summary>
        /// Used when specifying split rows on table creation.
        /// </summary>
        SplitRow = Type.SplitRow,
        /// <summary>
        /// Used when specifying an inclusive lower bound range on table creation.
        /// Should be followed by the associated upper bound. If all values are
        /// missing, then signifies unbounded.
        /// </summary>
        RangeLowerBound = Type.RangeLowerBound,
        /// <summary>
        /// Used when specifying an exclusive upper bound range on table creation.
        /// Should be preceded by the associated lower bound. If all values are
        /// missing, then signifies unbounded.
        /// </summary>
        RangeUpperBound = Type.RangeUpperBound,
        /// <summary>
        /// Used when specifying an exclusive lower bound range on table creation.
        /// Should be followed by the associated upper bound. If all values are
        /// missing, then signifies unbounded.
        /// </summary>
        ExclusiveRangeLowerBound = Type.ExclusiveRangeLowerBound,
        /// <summary>
        /// Used when specifying an inclusive upper bound range on table creation.
        /// Should be preceded by the associated lower bound. If all values are
        /// missing, then signifies unbounded.
        /// </summary>
        InclusiveRangeUpperBound = Type.InclusiveRangeUpperBound,
    }
}
