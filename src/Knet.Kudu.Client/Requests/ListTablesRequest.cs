using System.Buffers;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests;

internal sealed class ListTablesRequest : KuduMasterRpc<ListTablesResponsePB>
{
    private static readonly ListTablesRequestPB _noFilterRequest = new();

    private readonly ListTablesRequestPB _request;

    public ListTablesRequest(string nameFilter = null)
    {
        MethodName = "ListTables";

        _request = nameFilter is null
            ? _noFilterRequest
            : new ListTablesRequestPB { NameFilter = nameFilter };
    }

    public override int CalculateSize() => _request.CalculateSize();

    public override void WriteTo(IBufferWriter<byte> output) => _request.WriteTo(output);

    public override void ParseResponse(KuduMessage message)
    {
        Output = ListTablesResponsePB.Parser.ParseFrom(message.MessageProtobuf);
        Error = Output.Error;
    }
}
