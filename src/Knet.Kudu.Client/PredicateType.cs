namespace Knet.Kudu.Client;

public enum PredicateType
{
    /// <summary>
    /// A predicate which filters all rows.
    /// </summary>
    None,
    /// <summary>
    /// A predicate which filters all rows not equal to a value.
    /// </summary>
    Equality,
    /// <summary>
    /// A predicate which filters all rows not in a range.
    /// </summary>
    Range,
    /// <summary>
    /// A predicate which filters all null rows.
    /// </summary>
    IsNotNull,
    /// <summary>
    /// A predicate which filters all non-null rows.
    /// </summary>
    IsNull,
    /// <summary>
    /// A predicate which filters all rows not matching a list of values.
    /// </summary>
    InList,
    /// <summary>
    /// A predicate which evaluates to true if the column value is present in
    /// a bloom filter.
    /// </summary>
    InBloomFilter
}
