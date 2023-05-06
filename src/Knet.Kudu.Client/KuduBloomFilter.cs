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
        uint hash = GetBoolHash(value);
        BloomFilter.Insert(hash);
    }

    public void AddSByte(sbyte value)
    {
        uint hash = GetSByteHash(value);
        BloomFilter.Insert(hash);
    }

    public void AddInt16(short value)
    {
        uint hash = GetInt16Hash(value);
        BloomFilter.Insert(hash);
    }

    public void AddInt32(int value)
    {
        uint hash = GetInt32Hash(value);
        BloomFilter.Insert(hash);
    }

    public void AddInt64(long value)
    {
        uint hash = GetInt64Hash(value);
        BloomFilter.Insert(hash);
    }

    public void AddDateTime(DateTime value)
    {
        uint hash = GetDateTimeHash(value);
        BloomFilter.Insert(hash);
    }

    // TODO: DateOnly

    public void AddFloat(float value)
    {
        uint hash = GetFloatHash(value);
        BloomFilter.Insert(hash);
    }

    public void AddDouble(double value)
    {
        uint hash = GetDoubleHash(value);
        BloomFilter.Insert(hash);
    }

    public void AddDecimal(decimal value)
    {
        uint hash = GetDecimalHash(value);
        BloomFilter.Insert(hash);
    }

    public void AddString(string value)
    {
        uint hash = GetStringHash(value);
        BloomFilter.Insert(hash);
    }

    public void AddBinary(ReadOnlySpan<byte> value)
    {
        uint hash = GetBinaryHash(value);
        BloomFilter.Insert(hash);
    }

    public bool FindBool(bool value)
    {
        uint hash = GetBoolHash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindSByte(sbyte value)
    {
        uint hash = GetSByteHash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindInt16(short value)
    {
        uint hash = GetInt16Hash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindInt32(int value)
    {
        uint hash = GetInt32Hash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindInt64(long value)
    {
        uint hash = GetInt64Hash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindDateTime(DateTime value)
    {
        uint hash = GetDateTimeHash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindFloat(float value)
    {
        uint hash = GetFloatHash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindDouble(double value)
    {
        uint hash = GetDoubleHash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindDecimal(decimal value)
    {
        uint hash = GetDecimalHash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindString(string value)
    {
        uint hash = GetStringHash(value);
        return BloomFilter.Find(hash);
    }

    public bool FindBinary(ReadOnlySpan<byte> value)
    {
        uint hash = GetBinaryHash(value);
        return BloomFilter.Find(hash);
    }

    private uint GetBoolHash(bool value)
    {
        CheckType(KuduType.Bool);
        Span<byte> span = stackalloc byte[1];
        KuduEncoder.EncodeBool(span, value);
        return FastHash.Hash32(span, _hashSeed);
    }

    private uint GetSByteHash(sbyte value)
    {
        CheckType(KuduType.Int8);
        Span<byte> span = stackalloc byte[1];
        KuduEncoder.EncodeInteger(span, value);
        return FastHash.Hash32(span, _hashSeed);
    }

    private uint GetInt16Hash(short value)
    {
        CheckType(KuduType.Int16);
        Span<byte> span = stackalloc byte[2];
        KuduEncoder.EncodeInteger(span, value);
        return FastHash.Hash32(span, _hashSeed);
    }

    private uint GetInt32Hash(int value)
    {
        CheckType(KuduTypeFlags.Int32 | KuduTypeFlags.Date);
        Span<byte> span = stackalloc byte[4];
        KuduEncoder.EncodeInteger(span, value);
        return FastHash.Hash32(span, _hashSeed);
    }

    private uint GetInt64Hash(long value)
    {
        CheckType(KuduTypeFlags.Int64 | KuduTypeFlags.UnixtimeMicros);
        Span<byte> span = stackalloc byte[8];
        KuduEncoder.EncodeInteger(span, value);
        return FastHash.Hash32(span, _hashSeed);
    }

    private uint GetDateTimeHash(DateTime value)
    {
        var column = Column;

        switch (column.Type)
        {
            case KuduType.UnixtimeMicros:
                {
                    Span<byte> span = stackalloc byte[8];
                    KuduEncoder.EncodeDateTime(span, value);
                    return FastHash.Hash32(span, _hashSeed);
                }
            case KuduType.Date:
                {
                    Span<byte> span = stackalloc byte[4];
                    KuduEncoder.EncodeDate(span, value);
                    return FastHash.Hash32(span, _hashSeed);
                }
            default:
                {
                    return KuduTypeValidation.ThrowException<uint>(column,
                        KuduTypeFlags.UnixtimeMicros | KuduTypeFlags.Date);
                }
        }
    }

    private uint GetFloatHash(float value)
    {
        CheckType(KuduType.Float);
        Span<byte> span = stackalloc byte[4];
        KuduEncoder.EncodeFloat(span, value);
        return FastHash.Hash32(span, _hashSeed);
    }

    private uint GetDoubleHash(double value)
    {
        CheckType(KuduType.Double);
        Span<byte> span = stackalloc byte[8];
        KuduEncoder.EncodeDouble(span, value);
        return FastHash.Hash32(span, _hashSeed);
    }

    private uint GetDecimalHash(decimal value)
    {
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
                    return FastHash.Hash32(span, _hashSeed);
                }
            case KuduType.Decimal64:
                {
                    Span<byte> span = stackalloc byte[8];
                    KuduEncoder.EncodeDecimal64(span, value, precision, scale);
                    return FastHash.Hash32(span, _hashSeed);
                }
            case KuduType.Decimal128:
                {
                    Span<byte> span = stackalloc byte[16];
                    KuduEncoder.EncodeDecimal128(span, value, precision, scale);
                    return FastHash.Hash32(span, _hashSeed);
                }
            default:
                {
                    return KuduTypeValidation.ThrowException<uint>(column,
                        KuduTypeFlags.Decimal32 | KuduTypeFlags.Decimal64 | KuduTypeFlags.Decimal128);
                }

        }
    }

    private uint GetStringHash(string value)
    {
        // TODO: Substring varchar?
        CheckType(KuduTypeFlags.String | KuduTypeFlags.Varchar);

        var length = Encoding.UTF8.GetByteCount(value);

        byte[]? unescapedArray = null;

        Span<byte> span = length <= StackallocByteThreshold
            ? stackalloc byte[StackallocByteThreshold]
            : (unescapedArray = ArrayPool<byte>.Shared.Rent(length));

        var written = Encoding.UTF8.GetBytes(value, span);
        var slice = span.Slice(0, written);
        uint hash = FastHash.Hash32(slice, _hashSeed);

        if (unescapedArray is not null)
        {
            ArrayPool<byte>.Shared.Return(unescapedArray);
        }

        return hash;
    }

    private uint GetBinaryHash(ReadOnlySpan<byte> value)
    {
        CheckType(KuduType.Binary);
        return FastHash.Hash32(value, _hashSeed);
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
