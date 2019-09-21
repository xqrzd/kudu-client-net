using System.Buffers;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Util;
using ProtoBuf;
using ProtoBuf.Meta;

namespace Kudu.Client.Connection
{
    public static class KuduProtocol
    {
        public static bool TryParseMessage(
            ref ReadOnlySequence<byte> buffer, ParserContext parserContext)
        {
            switch (parserContext.Step)
            {
                case ParseStep.NotStarted:
                    {
                        if (buffer.TryReadInt32BigEndian(out parserContext.TotalMessageLength))
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
                        if (buffer.TryReadVarintUInt32(out parserContext.HeaderLength))
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
                        if (TryParseResponseHeader(
                            ref buffer, parserContext.HeaderLength, out parserContext.Header))
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
                        if (buffer.TryReadVarintUInt32(out parserContext.MainMessageLength))
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
                        if (buffer.Length < messageLength)
                        {
                            // Not enough data to parse main protobuf message.
                            parserContext.Step = ParseStep.ReadProtobufMessage;
                            break;
                        }

                        parserContext.Step = ParseStep.NotStarted;

                        return true;
                    }
            }

            return false;
        }

        private static bool TryParseResponseHeader(
            ref ReadOnlySequence<byte> buffer, long length, out ResponseHeader header)
        {
            if (buffer.Length < length)
            {
                header = null;
                return false;
            }

            ReadOnlySequence<byte> slice = buffer.Slice(0, length);
            using var reader = ProtoReader.Create(
                out var state, slice, RuntimeTypeModel.Default);

            header = reader.Deserialize<ResponseHeader>(ref state);
            buffer = buffer.Slice(length);

            return true;
        }

        public static ErrorStatusPB ParseError(ReadOnlySequence<byte> buffer)
        {
            using var reader = ProtoReader.Create(
                out var state, buffer, RuntimeTypeModel.Default);

            return reader.Deserialize<ErrorStatusPB>(ref state);
        }
    }
}
