using System;
using System.Buffers;
using System.Text;
using Google.Protobuf;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client;

public class KuduBloomFilter
{
    private const int StackallocByteThreshold = 256;

    private readonly uint _hashSeed;

    internal ColumnSchema Column { get; }

    internal BlockBloomFilter BloomFilter { get; }

    public KuduBloomFilter(BlockBloomFilter bloomFilter, uint hashSeed, ColumnSchema column)
    {
        BloomFilter = bloomFilter;
        _hashSeed = hashSeed;
        Column = column;
    }

    public BlockBloomFilterPB ToProtobuf()
    {
        return new BlockBloomFilterPB
        {
            HashAlgorithm = HashAlgorithm.FastHash,
            HashSeed = _hashSeed,
            AlwaysFalse = BloomFilter.AlwaysFalse,
            LogSpaceBytes = BloomFilter.LogSpaceBytes,
            BloomData = UnsafeByteOperations.UnsafeWrap(BloomFilter.Memory)
        };
    }

    public void AddBool(bool value)
    {
        CheckType(KuduType.Bool);
        Span<byte> span = stackalloc byte[1];
        KuduEncoder.EncodeBool(span, value);
        AddSpan(span);
    }

    public void AddSByte(sbyte value)
    {
        CheckType(KuduType.Int8);
        Span<byte> span = stackalloc byte[1];
        KuduEncoder.EncodeInteger(span, value);
        AddSpan(span);
    }

    public void AddInt16(short value)
    {
        CheckType(KuduType.Int16);
        Span<byte> span = stackalloc byte[2];
        KuduEncoder.EncodeInteger(span, value);
        AddSpan(span);
    }

    public void AddInt32(int value)
    {
        CheckType(KuduTypeFlags.Int32 | KuduTypeFlags.Date);
        Span<byte> span = stackalloc byte[4];
        KuduEncoder.EncodeInteger(span, value);
        AddSpan(span);
    }

    public void AddInt64(long value)
    {
        CheckType(KuduTypeFlags.Int64 | KuduTypeFlags.UnixtimeMicros);
        Span<byte> span = stackalloc byte[8];
        KuduEncoder.EncodeInteger(span, value);
        AddSpan(span);
    }

    public void AddDateTime(DateTime value)
    {
        var type = Column.Type;

        if (type == KuduType.UnixtimeMicros)
        {
            Span<byte> span = stackalloc byte[8];
            KuduEncoder.EncodeDateTime(span, value);
            AddSpan(span);
        }
        else if (type == KuduType.Date)
        {
            Span<byte> span = stackalloc byte[4];
            KuduEncoder.EncodeDate(span, value);
            AddSpan(span);
        }
        else
        {
            KuduTypeValidation.ThrowException(Column,
                KuduTypeFlags.UnixtimeMicros |
                KuduTypeFlags.Date);
        }
    }

    // TODO: DateOnly

    public void AddFloat(float value)
    {
        CheckType(KuduType.Float);
        Span<byte> span = stackalloc byte[4];
        KuduEncoder.EncodeFloat(span, value);
        AddSpan(span);
    }

    public void AddDouble(double value)
    {
        CheckType(KuduType.Double);
        Span<byte> span = stackalloc byte[8];
        KuduEncoder.EncodeDouble(span, value);
        AddSpan(span);
    }

    public void AddDecimal(decimal value)
    {
        CheckType(KuduTypeFlags.Decimal32 | KuduTypeFlags.Decimal64 | KuduTypeFlags.Decimal128);

        var column = Column;
        var attributes = column.TypeAttributes!;
        var precision = attributes.Precision.GetValueOrDefault();
        var scale = attributes.Scale.GetValueOrDefault();

        switch (column.Type)
        {
            case KuduType.Decimal32:
                {
                    Span<byte> span = stackalloc byte[4];
                    KuduEncoder.EncodeDecimal32(span, value, precision, scale);
                    AddSpan(span);
                    break;
                }
            case KuduType.Decimal64:
                {
                    Span<byte> span = stackalloc byte[8];
                    KuduEncoder.EncodeDecimal64(span, value, precision, scale);
                    AddSpan(span);
                    break;
                }
            case KuduType.Decimal128:
                {
                    Span<byte> span = stackalloc byte[16];
                    KuduEncoder.EncodeDecimal128(span, value, precision, scale);
                    AddSpan(span);
                    break;
                }
        }
    }

    public void AddString(string value)
    {
        // TODO: Substring varchar?
        CheckType(KuduTypeFlags.String | KuduTypeFlags.Varchar);

        var length = Encoding.UTF8.GetByteCount(value);

        byte[]? unescapedArray = null;

        Span<byte> span = length <= StackallocByteThreshold
            ? stackalloc byte[StackallocByteThreshold]
            : (unescapedArray = ArrayPool<byte>.Shared.Rent(length));

        var written = Encoding.UTF8.GetBytes(value, span);
        AddSpan(span.Slice(0, written));

        if (unescapedArray is not null)
        {
            ArrayPool<byte>.Shared.Return(unescapedArray);
        }
    }

    public void AddBinary(ReadOnlySpan<byte> value)
    {
        CheckType(KuduType.Binary);
        AddSpan(value);
    }

    // TODO: Add remaining find methods

    public bool FindInt64(long value)
    {
        Span<byte> span = stackalloc byte[8];
        KuduEncoder.EncodeInteger(span, value);
        uint hash = FastHash.Hash32(span, _hashSeed);
        return BloomFilter.Find(hash);
    }

    public bool FindBinary(ReadOnlySpan<byte> value)
    {
        uint hash = FastHash.Hash32(value, _hashSeed);
        return BloomFilter.Find(hash);
    }

    private void AddSpan(ReadOnlySpan<byte> value)
    {
        uint hash = FastHash.Hash32(value, _hashSeed);
        BloomFilter.Insert(hash);
    }

    private void CheckType(KuduType type)
    {
        KuduTypeValidation.ValidateColumnType(Column, type);
    }

    private void CheckType(KuduTypeFlags typeFlags)
    {
        KuduTypeValidation.ValidateColumnType(Column, typeFlags);
    }
}
