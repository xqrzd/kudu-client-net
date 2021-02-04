namespace Knet.Kudu.Client
{
    public class HiveMetastoreConfig
    {
        public string HiveMetastoreUris { get; }

        public bool HiveMetastoreSaslEnabled { get; }

        public string HiveMetastoreUuid { get; }

        public HiveMetastoreConfig(
            string hiveMetastoreUris,
            bool hiveMetastoreSaslEnabled,
            string hiveMetastoreUuid)
        {
            HiveMetastoreUris = hiveMetastoreUris;
            HiveMetastoreSaslEnabled = hiveMetastoreSaslEnabled;
            HiveMetastoreUuid = hiveMetastoreUuid;
        }
    }
}
