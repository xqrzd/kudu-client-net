using System;

namespace Kudu.Client
{
    public class ResultSet
    {
        private readonly ReadOnlyMemory<byte> _buffer;

        public int NumRows { get; }

        public ResultSet(
            ReadOnlyMemory<byte> buffer,
            int numRows)
        {
            _buffer = buffer;
            NumRows = numRows;
        }
    }
}
