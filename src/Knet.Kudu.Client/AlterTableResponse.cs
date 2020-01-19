namespace Knet.Kudu.Client
{
    public class AlterTableResponse
    {
        public string TableId { get; }

        public uint SchemaVersion { get; }

        public AlterTableResponse(string tableId, uint schemaVersion)
        {
            TableId = tableId;
            SchemaVersion = schemaVersion;
        }
    }
}
