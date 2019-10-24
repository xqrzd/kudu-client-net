using System.Buffers;
using Kudu.Client.Exceptions;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Util;
using ProtoBuf;

namespace Kudu.Client.Connection
{
    internal static class KuduProtocol
    {
        public static bool TryParseMessage(
            ref SequenceReader<byte> reader, ParserContext parserContext)
        {
            switch (parserContext.Step)
            {
                case ParseStep.NotStarted:
                    {
                        if (reader.TryReadBigEndian(out parserContext.TotalMessageLength))
                        {
                            goto case ParseStep.ReadHeaderLength;
                        }
                        else
                        {
                            // Not enough data to read message size.
                            break;
                        }
                    }
                case ParseStep.ReadHeaderLength:
                    {
                        if (reader.TryReadVarint(out parserContext.HeaderLength))
                        {
                            goto case ParseStep.ReadHeader;
                        }
                        else
                        {
                            // Not enough data to read header length.
                            parserContext.Step = ParseStep.ReadHeaderLength;
                            break;
                        }
                    }
                case ParseStep.ReadHeader:
                    {
                        if (TryParseResponseHeader(ref reader,
                            parserContext.HeaderLength, out parserContext.Header))
                        {
                            goto case ParseStep.ReadMainMessageLength;
                        }
                        else
                        {
                            // Not enough data to read header.
                            parserContext.Step = ParseStep.ReadHeader;
                            break;
                        }
                    }
                case ParseStep.ReadMainMessageLength:
                    {
                        if (reader.TryReadVarint(out parserContext.MainMessageLength))
                        {
                            goto case ParseStep.ReadProtobufMessage;
                        }
                        else
                        {
                            // Not enough data to read main message length.
                            parserContext.Step = ParseStep.ReadMainMessageLength;
                            break;
                        }
                    }
                case ParseStep.ReadProtobufMessage:
                    {
                        var messageLength = parserContext.ProtobufMessageLength;
                        if (reader.Remaining < messageLength)
                        {
                            // Not enough data to parse main protobuf message.
                            parserContext.Step = ParseStep.ReadProtobufMessage;
                            break;
                        }

                        parserContext.MainProtobufMessage = reader.Sequence
                            .Slice(reader.Position, messageLength);

                        reader.Advance(messageLength);

                        parserContext.Step = ParseStep.NotStarted;

                        return true;
                    }
            }

            return false;
        }

        public static RpcException GetRpcError(ParserContext parserContext)
        {
            var buffer = parserContext.MainProtobufMessage;
            var error = Serializer.Deserialize<ErrorStatusPB>(buffer);
            return new RpcException(error);
        }

        private static bool TryParseResponseHeader(
            ref SequenceReader<byte> reader, long length, out ResponseHeader header)
        {
            if (reader.Remaining < length)
            {
                header = null;
                return false;
            }

            var slice = reader.Sequence.Slice(reader.Position, length);
            header = Serializer.Deserialize<ResponseHeader>(slice);

            reader.Advance(length);

            return true;
        }
    }
}
