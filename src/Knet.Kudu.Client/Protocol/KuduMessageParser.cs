using System.Buffers;

namespace Knet.Kudu.Client.Protocol
{
    internal static class KuduMessageParser
    {
        // +------------------------------------------------+
        // | Total message length (4 bytes)                 |
        // +------------------------------------------------+
        // | RPC Header protobuf length (variable encoding) |
        // +------------------------------------------------+
        // | RPC Header protobuf                            |
        // +------------------------------------------------+
        // | Main message length (variable encoding)        |
        // +------------------------------------------------+ --- 0
        // | Main message protobuf                          |
        // +------------------------------------------------+ --- sidecar_offsets(0)
        // | Sidecar 0                                      |
        // +------------------------------------------------+ --- sidecar_offsets(1)
        // | Sidecar 1                                      |
        // +------------------------------------------------+ --- sidecar_offsets(2)
        // | Sidecar 2                                      |
        // +------------------------------------------------+ --- ...
        // | ...                                            |
        // +------------------------------------------------+

        public static bool TryParse(ref SequenceReader<byte> reader, ParserContext parserContext)
        {
            switch (parserContext.Step)
            {
                case ParseStep.TotalMessageLength:
                    if (parserContext.TryReadTotalMessageLength(ref reader))
                    {
                        goto case ParseStep.HeaderLength;
                    }
                    else
                    {
                        // Not enough data to read the message size.
                        break;
                    }
                case ParseStep.HeaderLength:
                    if (parserContext.TryReadHeaderLength(ref reader))
                    {
                        goto case ParseStep.Header;
                    }
                    else
                    {
                        // Not enough data to read the header length.
                        parserContext.Step = ParseStep.HeaderLength;
                        break;
                    }
                case ParseStep.Header:
                    if (parserContext.TryReadResponseHeader(ref reader))
                    {
                        goto case ParseStep.MainMessageLength;
                    }
                    else
                    {
                        // Not enough data to read the header.
                        parserContext.Step = ParseStep.Header;
                        break;
                    }
                case ParseStep.MainMessageLength:
                    if (parserContext.TryReadMessageLength(ref reader))
                    {
                        goto case ParseStep.MainMessage;
                    }
                    else
                    {
                        // Not enough data to read the main message length.
                        parserContext.Step = ParseStep.MainMessageLength;
                        break;
                    }
                case ParseStep.MainMessage:
                    if (parserContext.TryReadMainMessage(ref reader))
                    {
                        return true;
                    }
                    else
                    {
                        // Not enough data to read the main message.
                        parserContext.Step = ParseStep.MainMessage;
                        break;
                    }
            }

            return false;
        }
    }
}
