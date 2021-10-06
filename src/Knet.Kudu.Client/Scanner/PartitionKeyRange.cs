namespace Knet.Kudu.Client.Scanner;

public readonly struct PartitionKeyRange
{
    public byte[] Lower { get; }

    public byte[] Upper { get; }

    public PartitionKeyRange(byte[] lower, byte[] upper)
    {
        Lower = lower;
        Upper = upper;
    }
}
