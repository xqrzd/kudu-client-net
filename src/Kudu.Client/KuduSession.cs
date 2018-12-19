using System.Collections.Generic;
using System.Threading.Tasks;
using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client
{
    public class KuduSession
    {
        private readonly KuduClient _client;
        private readonly List<Operation> _operations;

        public KuduSession(KuduClient client)
        {
            _client = client;
            _operations = new List<Operation>();
        }

        public void Apply(Operation operation)
        {
            _operations.Add(operation);
        }

        public async Task<WriteResponsePB> FlushAsync()
        {
            // Just testing, assume every operation is for the same tablet.
            var op = _operations[0];
            var tablet = await _client.GetRowTabletAsync(op.Table, op.Row).ConfigureAwait(false);

            return await _client.WriteRowAsync(_operations, tablet).ConfigureAwait(false);
        }
    }
}
