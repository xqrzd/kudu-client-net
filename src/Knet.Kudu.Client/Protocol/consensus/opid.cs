// This file was generated by a tool; you should avoid making direct changes.
// Consider using 'partial classes' to extend these types
// Input: opid.proto

#pragma warning disable CS1591, CS0612, CS3021, IDE1006
namespace Knet.Kudu.Client.Protocol.Consensus
{

    [global::ProtoBuf.ProtoContract()]
    public partial class OpId : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"term", IsRequired = true)]
        public long Term { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"index", IsRequired = true)]
        public long Index { get; set; }

    }

}

#pragma warning restore CS1591, CS0612, CS3021, IDE1006