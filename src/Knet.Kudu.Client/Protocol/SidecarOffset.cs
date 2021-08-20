namespace Knet.Kudu.Client.Protocol
{
    public readonly struct SidecarOffset
    {
        public readonly int Start;

        public readonly int Length;

        public SidecarOffset(int start, int length)
        {
            Start = start;
            Length = length;
        }

        public override string ToString() =>
            $"Start: {Start}, Length: {Length}";
    }
}
