using static Knet.Kudu.Client.Protobuf.Consensus.RaftPeerPB.Types;

namespace Knet.Kudu.Client
{
    public enum ReplicaRole
    {
        Follower = Role.Follower,
        Leader = Role.Leader,
        Learner = Role.Learner,
        NonParticipant = Role.NonParticipant
    }
}
