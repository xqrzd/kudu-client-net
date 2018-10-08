using System.Collections.Generic;
using Kudu.Client.Protocol;

namespace Kudu.Client.Builder
{
    public class ScanBuilder
    {
        internal KuduClient Client { get; }
        internal KuduTable Table { get; }
        internal List<string> ProjectedColumns { get; }
        internal ReadMode ReadMode { get; private set; }
        internal int BatchSizeBytes { get; private set; }

        public ScanBuilder(KuduClient client, KuduTable table)
        {
            Client = client;
            Table = table;
            ProjectedColumns = new List<string>();
        }

        public ScanBuilder AddProjectedColumn(string column)
        {
            ProjectedColumns.Add(column);
            return this;
        }

        public ScanBuilder SetReadMode(KuduReadMode readMode)
        {
            ReadMode = (ReadMode)readMode;
            return this;
        }

        public ScanBuilder SetBatchSizeBytes(int batchSizeBytes)
        {
            BatchSizeBytes = batchSizeBytes;
            return this;
        }

        public KuduScanner Build() => new KuduScanner(this);
    }
}
