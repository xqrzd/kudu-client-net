namespace Knet.Kudu.Client
{
    public class HiveMetastoreConfig
    {
        /// <summary>
        /// Address(es) of the Hive Metastore instance(s).
        /// 
        /// For more info see the Kudu master --hive_metastore_uris flag for more info,
        /// or the Hive Metastore hive.metastore.uris configuration.
        /// </summary>
        public string HiveMetastoreUris { get; }

        /// <summary>
        /// Whether the Hive Metastore instance uses SASL (Kerberos) security.
        /// 
        /// For more info see the Kudu master --hive_metastore_sasl_enabled flag, or
        /// the Hive Metastore hive.metastore.sasl.enabled configuration.
        /// </summary>
        public bool HiveMetastoreSaslEnabled { get; }

        /// <summary>
        /// An ID which uniquely identifies the Hive Metastore instance.
        /// 
        /// NOTE: this is provided on a best-effort basis, as not all Hive Metastore
        /// versions which Kudu is compatible with include the necessary APIs. See
        /// HIVE-16452 for more info.
        /// </summary>
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
