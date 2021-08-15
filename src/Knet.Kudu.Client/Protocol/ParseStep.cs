namespace Knet.Kudu.Client.Protocol
{
    internal enum ParseStep
    {
        /// <summary>
        /// Total message length (4 bytes).
        /// </summary>
        TotalMessageLength,
        /// <summary>
        /// RPC Header protobuf length (variable encoding).
        /// </summary>
        HeaderLength,
        /// <summary>
        /// RPC Header protobuf.
        /// </summary>
        Header,
        /// <summary>
        /// Main message length (variable encoding),
        /// including sidecars (if any).
        /// </summary>
        MainMessageLength,
        /// <summary>
        /// Main message protobuf, including any sidecars.
        /// </summary>
        MainMessage
    }
}
