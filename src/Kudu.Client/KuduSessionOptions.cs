using System;

namespace Kudu.Client
{
    public class KuduSessionOptions
    {
        public int BatchSize { get; set; } = 1000;

        public int Capacity { get; set; } = 100000;

        public bool SingleWriter { get; set; }

        public bool IgnoreAllDuplicateRows { get; set; }

        public bool IgnoreAllNotFoundRows { get; set; }

        public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(1);

        public ExternalConsistencyMode ExternalConsistencyMode { get; set; } =
            ExternalConsistencyMode.ClientPropagated;
    }
}
