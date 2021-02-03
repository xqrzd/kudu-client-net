using System.Collections.Generic;
using System.Diagnostics;

namespace Knet.Kudu.Client.Connection
{
    public class RequestTracker
    {
        public const long NoSeqNo = -1;

        private readonly SortedSet<long> _incompleteRpcs;
        private long _nextSeqNo;

        public string ClientId { get; }

        public RequestTracker(string clientId)
        {
            ClientId = clientId;
            _incompleteRpcs = new SortedSet<long>();
            _nextSeqNo = 1;
        }

        /// <summary>
        /// Returns the oldest sequence number that wasn't marked as completed.
        /// If there is no incomplete RPC then <see cref="NoSeqNo"/> is returned.
        /// </summary>
        public long FirstIncomplete
        {
            get
            {
                lock (_incompleteRpcs)
                {
                    if (_incompleteRpcs.Count == 0)
                        return NoSeqNo;

                    return _incompleteRpcs.Min;
                }
            }
        }

        /// <summary>
        /// Generates a new sequence number and tracks it.
        /// </summary>
        public long GetNewSeqNo()
        {
            lock (_incompleteRpcs)
            {
                long seq = _nextSeqNo++;
                _incompleteRpcs.Add(seq);
                return seq;
            }
        }

        /// <summary>
        /// Marks the given sequence number as complete. The provided sequence number must
        /// be a valid number that was previously returned by <see cref="GetNewSeqNo"/>.
        /// It is illegal to call this method twice with the same sequence number.
        /// </summary>
        /// <param name="sequenceNo">The sequence number to mark as complete.</param>
        public void CompleteRpc(long sequenceNo)
        {
            Debug.Assert(sequenceNo != NoSeqNo);
            lock (_incompleteRpcs)
            {
                bool removed = _incompleteRpcs.Remove(sequenceNo);
                Debug.Assert(removed, $"Could not remove seqid {sequenceNo} from request tracker");
            }
        }
    }
}
