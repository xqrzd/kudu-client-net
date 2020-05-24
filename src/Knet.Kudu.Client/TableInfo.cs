namespace Knet.Kudu.Client
{
    public class TableInfo
    {
        /// <summary>
        /// The table name.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// The table Id.
        /// </summary>
        public string TableId { get; }

        public TableInfo(string tableName, string tableId)
        {
            TableName = tableName;
            TableId = tableId;
        }

        public override string ToString() => TableName;
    }
}
