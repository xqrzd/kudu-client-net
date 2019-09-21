namespace Kudu.Client.Scanner
{
    /// <summary>
    /// Helper object that contains all the info sent by a TS after a Scan request.
    /// </summary>
    internal class ScanResponse<T>
    {
        /// <summary>
        /// The ID associated with the scanner that issued the request.
        /// </summary>
        public byte[] ScannerId { get; }

        /// <summary>
        /// The actual payload of the response.
        /// </summary>
        public T Data { get; set; }

        /// <summary>
        /// Number of rows returned by the scanner.
        /// </summary>
        public int NumRows { get; }

        /// <summary>
        /// If false, the filter we use decided there was no more data to scan.
        /// In this case, the server has automatically closed the scanner for us,
        /// so we don't need to explicitly close it.
        /// </summary>
        public bool HasMoreResults { get; }

        /// <summary>
        /// Server-assigned timestamp for the scan operation. It's used when
        /// the scan operates in READ_AT_SNAPSHOT mode and the timestamp is not
        /// specified explicitly. The field is set with the snapshot timestamp sent
        /// in the response from the very first tablet server contacted while
        /// fetching data from corresponding tablets. If the tablet server does not
        /// send the snapshot timestamp in its response, this field is assigned
        /// a special value <see cref="KuduClient.NoTimestamp"/>.
        /// </summary>
        public long ScanTimestamp { get; }

        /// <summary>
        /// The server timestamp to propagate, if set. If the server response does
        /// not contain propagated timestamp, this field is set to special value
        /// <see cref="KuduClient.NoTimestamp"/>.
        /// </summary>
        public long PropagatedTimestamp { get; }

        /// <summary>
        /// If this is a fault-tolerant scanner, this is set to the encoded primary
        /// key of the last row returned in the response.
        /// </summary>
        public byte[] LastPrimaryKey { get; }

        public ScanResponse(
            byte[] scannerId,
            int numRows,
            bool hasMoreResults,
            long scanTimestamp,
            long propagatedTimestamp,
            byte[] lastPrimaryKey)
        {
            ScannerId = scannerId;
            NumRows = numRows;
            HasMoreResults = hasMoreResults;
            ScanTimestamp = scanTimestamp;
            PropagatedTimestamp = propagatedTimestamp;
            LastPrimaryKey = lastPrimaryKey;
        }
    }
}
