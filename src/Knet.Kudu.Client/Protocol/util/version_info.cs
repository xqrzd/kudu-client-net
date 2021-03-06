// This file was generated by a tool; you should avoid making direct changes.
// Consider using 'partial classes' to extend these types
// Input: version_info.proto

#pragma warning disable CS1591, CS0612, CS3021, IDE1006
namespace Knet.Kudu.Client.Protocol
{

    [global::ProtoBuf.ProtoContract()]
    public partial class VersionInfoPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"git_hash")]
        [global::System.ComponentModel.DefaultValue("")]
        public string GitHash
        {
            get { return __pbn__GitHash ?? ""; }
            set { __pbn__GitHash = value; }
        }
        public bool ShouldSerializeGitHash() => __pbn__GitHash != null;
        public void ResetGitHash() => __pbn__GitHash = null;
        private string __pbn__GitHash;

        [global::ProtoBuf.ProtoMember(2, Name = @"build_hostname")]
        [global::System.ComponentModel.DefaultValue("")]
        public string BuildHostname
        {
            get { return __pbn__BuildHostname ?? ""; }
            set { __pbn__BuildHostname = value; }
        }
        public bool ShouldSerializeBuildHostname() => __pbn__BuildHostname != null;
        public void ResetBuildHostname() => __pbn__BuildHostname = null;
        private string __pbn__BuildHostname;

        [global::ProtoBuf.ProtoMember(3, Name = @"build_timestamp")]
        [global::System.ComponentModel.DefaultValue("")]
        public string BuildTimestamp
        {
            get { return __pbn__BuildTimestamp ?? ""; }
            set { __pbn__BuildTimestamp = value; }
        }
        public bool ShouldSerializeBuildTimestamp() => __pbn__BuildTimestamp != null;
        public void ResetBuildTimestamp() => __pbn__BuildTimestamp = null;
        private string __pbn__BuildTimestamp;

        [global::ProtoBuf.ProtoMember(4, Name = @"build_username")]
        [global::System.ComponentModel.DefaultValue("")]
        public string BuildUsername
        {
            get { return __pbn__BuildUsername ?? ""; }
            set { __pbn__BuildUsername = value; }
        }
        public bool ShouldSerializeBuildUsername() => __pbn__BuildUsername != null;
        public void ResetBuildUsername() => __pbn__BuildUsername = null;
        private string __pbn__BuildUsername;

        [global::ProtoBuf.ProtoMember(5, Name = @"build_clean_repo")]
        public bool BuildCleanRepo
        {
            get { return __pbn__BuildCleanRepo.GetValueOrDefault(); }
            set { __pbn__BuildCleanRepo = value; }
        }
        public bool ShouldSerializeBuildCleanRepo() => __pbn__BuildCleanRepo != null;
        public void ResetBuildCleanRepo() => __pbn__BuildCleanRepo = null;
        private bool? __pbn__BuildCleanRepo;

        [global::ProtoBuf.ProtoMember(6, Name = @"build_id")]
        [global::System.ComponentModel.DefaultValue("")]
        public string BuildId
        {
            get { return __pbn__BuildId ?? ""; }
            set { __pbn__BuildId = value; }
        }
        public bool ShouldSerializeBuildId() => __pbn__BuildId != null;
        public void ResetBuildId() => __pbn__BuildId = null;
        private string __pbn__BuildId;

        [global::ProtoBuf.ProtoMember(7, Name = @"build_type")]
        [global::System.ComponentModel.DefaultValue("")]
        public string BuildType
        {
            get { return __pbn__BuildType ?? ""; }
            set { __pbn__BuildType = value; }
        }
        public bool ShouldSerializeBuildType() => __pbn__BuildType != null;
        public void ResetBuildType() => __pbn__BuildType = null;
        private string __pbn__BuildType;

        [global::ProtoBuf.ProtoMember(8, Name = @"version_string")]
        [global::System.ComponentModel.DefaultValue("")]
        public string VersionString
        {
            get { return __pbn__VersionString ?? ""; }
            set { __pbn__VersionString = value; }
        }
        public bool ShouldSerializeVersionString() => __pbn__VersionString != null;
        public void ResetVersionString() => __pbn__VersionString = null;
        private string __pbn__VersionString;

    }

}

#pragma warning restore CS1591, CS0612, CS3021, IDE1006
