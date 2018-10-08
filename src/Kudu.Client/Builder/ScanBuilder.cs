using System.Collections.Generic;
using Kudu.Client.Connection;
using Kudu.Client.Protocol;

namespace Kudu.Client.Builder
{
    public class ScanBuilder
    {
        internal KuduClient Client { get; }
        internal KuduTable Table { get; }
        internal List<string> ProjectedColumns { get; }
        internal ReadModePB ReadMode { get; private set; }
        internal ReplicaSelectionPB ReplicaSelection { get; private set; }
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

        public ScanBuilder SetReadMode(ReadMode readMode)
        {
            ReadMode = (ReadModePB)readMode;
            return this;
        }

        public ScanBuilder SetReplicaSelection(ReplicaSelection replicaSelection)
        {
            ReplicaSelection = (ReplicaSelectionPB)replicaSelection;
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
