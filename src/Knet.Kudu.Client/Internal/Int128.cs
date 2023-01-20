// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#if !NET7_0_OR_GREATER

using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Knet.Kudu.Client.Internal;

/// <summary>Represents a 128-bit signed integer.</summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct Int128 : IEquatable<Int128>, IComparable<Int128>
{
    internal const int Size = 16;

    private readonly ulong _lower;
    private readonly ulong _upper;

    /// <summary>Initializes a new instance of the <see cref="Int128" /> struct.</summary>
    /// <param name="upper">The upper 64-bits of the 128-bit value.</param>
    /// <param name="lower">The lower 64-bits of the 128-bit value.</param>
    public Int128(ulong upper, ulong lower)
    {
        _lower = lower;
        _upper = upper;
    }

    internal ulong Lower => _lower;

    internal ulong Upper => _upper;

    /// <inheritdoc cref="IComparable.CompareTo(object)" />
    public int CompareTo(object? value)
    {
        if (value is Int128 other)
        {
            return CompareTo(other);
        }
        else if (value is null)
        {
            return 1;
        }
        else
        {
            throw new ArgumentException("Object must be of type Int128.");
        }
    }

    /// <inheritdoc cref="IComparable{T}.CompareTo(T)" />
    public int CompareTo(Int128 value)
    {
        if (this < value)
        {
            return -1;
        }
        else if (this > value)
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    /// <inheritdoc cref="object.Equals(object?)" />
    public override bool Equals([NotNullWhen(true)] object? obj)
    {
        return (obj is Int128 other) && Equals(other);
    }

    /// <inheritdoc cref="IEquatable{T}.Equals(T)" />
    public bool Equals(Int128 other)
    {
        return this == other;
    }

    /// <inheritdoc cref="object.GetHashCode()" />
    public override int GetHashCode() => HashCode.Combine(_lower, _upper);

    //
    // Explicit Conversions From Int128
    //

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="byte" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="byte" />.</returns>
    public static explicit operator byte(Int128 value) => (byte)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="byte" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="byte" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked byte(Int128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((byte)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="char" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="char" />.</returns>
    public static explicit operator char(Int128 value) => (char)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="char" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="char" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked char(Int128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((char)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="decimal" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="decimal" />.</returns>
    public static explicit operator decimal(Int128 value)
    {
        if (IsNegative(value))
        {
            value = -value;
            return -(decimal)(UInt128)(value);
        }
        return (decimal)(UInt128)(value);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="double" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="double" />.</returns>
    public static explicit operator double(Int128 value)
    {
        if (IsNegative(value))
        {
            value = -value;
            return -(double)(UInt128)(value);
        }
        return (double)(UInt128)(value);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="short" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="short" />.</returns>
    public static explicit operator short(Int128 value) => (short)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="short" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="short" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked short(Int128 value)
    {
        if (~value._upper == 0)
        {
            long lower = (long)value._lower;
            return checked((short)lower);
        }

        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((short)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="int" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="int" />.</returns>
    public static explicit operator int(Int128 value) => (int)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="int" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="int" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked int(Int128 value)
    {
        if (~value._upper == 0)
        {
            long lower = (long)value._lower;
            return checked((int)lower);
        }

        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((int)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="long" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="long" />.</returns>
    public static explicit operator long(Int128 value) => (long)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="long" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="long" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked long(Int128 value)
    {
        if (~value._upper == 0)
        {
            long lower = (long)value._lower;
            return lower;
        }

        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((long)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="IntPtr" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="IntPtr" />.</returns>
    public static explicit operator nint(Int128 value) => (nint)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="IntPtr" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="IntPtr" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked nint(Int128 value)
    {
        if (~value._upper == 0)
        {
            long lower = (long)value._lower;
            return checked((nint)lower);
        }

        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((nint)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="sbyte" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="sbyte" />.</returns>
    public static explicit operator sbyte(Int128 value) => (sbyte)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="sbyte" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="sbyte" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked sbyte(Int128 value)
    {
        if (~value._upper == 0)
        {
            long lower = (long)value._lower;
            return checked((sbyte)lower);
        }

        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((sbyte)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="float" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="float" />.</returns>
    public static explicit operator float(Int128 value)
    {
        if (IsNegative(value))
        {
            value = -value;
            return -(float)(UInt128)(value);
        }
        return (float)(UInt128)(value);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="ushort" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ushort" />.</returns>
    public static explicit operator ushort(Int128 value) => (ushort)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="ushort" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ushort" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked ushort(Int128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((ushort)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="uint" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="uint" />.</returns>
    public static explicit operator uint(Int128 value) => (uint)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="uint" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="uint" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked uint(Int128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((uint)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="ulong" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ulong" />.</returns>
    public static explicit operator ulong(Int128 value) => value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="ulong" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ulong" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked ulong(Int128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return value._lower;
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="UInt128" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="UInt128" />.</returns>
    public static explicit operator UInt128(Int128 value) => new(value._upper, value._lower);

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="UInt128" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="UInt128" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked UInt128(Int128 value)
    {
        if ((long)value._upper < 0)
        {
            throw new OverflowException();
        }
        return new UInt128(value._upper, value._lower);
    }

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="UIntPtr" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="UIntPtr" />.</returns>
    public static explicit operator nuint(Int128 value) => (nuint)value._lower;

    /// <summary>Explicitly converts a 128-bit signed integer to a <see cref="UIntPtr" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="UIntPtr" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked nuint(Int128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((nuint)value._lower);
    }

    //
    // Explicit Conversions To Int128
    //

    /// <summary>Explicitly converts a <see cref="float" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static explicit operator Int128(float value) => (Int128)(double)(value);

    /// <summary>Explicitly converts a <see cref="float" /> value to a 128-bit signed integer, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="Int128" />.</exception>
    public static explicit operator checked Int128(float value) => checked((Int128)(double)(value));

    //
    // Implicit Conversions To Int128
    //

    /// <summary>Implicitly converts a <see cref="byte" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(byte value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="char" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(char value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="short" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(short value)
    {
        long lower = value;
        return new Int128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Implicitly converts a <see cref="int" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(int value)
    {
        long lower = value;
        return new Int128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Implicitly converts a <see cref="long" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(long value)
    {
        long lower = value;
        return new Int128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Implicitly converts a <see cref="IntPtr" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(nint value)
    {
        long lower = value;
        return new Int128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Implicitly converts a <see cref="sbyte" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(sbyte value)
    {
        long lower = value;
        return new Int128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Implicitly converts a <see cref="ushort" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(ushort value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="uint" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(uint value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="ulong" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(ulong value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="UIntPtr" /> value to a 128-bit signed integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit signed integer.</returns>
    public static implicit operator Int128(nuint value) => new(0, value);

    //
    // IAdditionOperators
    //

    public static Int128 operator +(Int128 left, Int128 right)
    {
        // For unsigned addition, we can detect overflow by checking `(x + y) < x`
        // This gives us the carry to add to upper to compute the correct result

        ulong lower = left._lower + right._lower;
        ulong carry = (lower < left._lower) ? 1UL : 0UL;

        ulong upper = left._upper + right._upper + carry;
        return new Int128(upper, lower);
    }

    public static Int128 operator checked +(Int128 left, Int128 right)
    {
        // For signed addition, we can detect overflow by checking if the sign of
        // both inputs are the same and then if that differs from the sign of the
        // output.

        Int128 result = left + right;

        uint sign = (uint)(left._upper >> 63);

        if (sign == (uint)(right._upper >> 63))
        {
            if (sign != (uint)(result._upper >> 63))
            {
                throw new OverflowException();
            }
        }
        return result;
    }

    //
    // IBinaryInteger
    //

    public static Int128 RotateLeft(Int128 value, int rotateAmount)
        => (value << rotateAmount) | (value >>> (128 - rotateAmount));

    public static Int128 RotateRight(Int128 value, int rotateAmount)
        => (value >>> rotateAmount) | (value << (128 - rotateAmount));

    //
    // IBitwiseOperators
    //

    public static Int128 operator &(Int128 left, Int128 right) => new(left._upper & right._upper, left._lower & right._lower);

    public static Int128 operator |(Int128 left, Int128 right) => new(left._upper | right._upper, left._lower | right._lower);

    public static Int128 operator ^(Int128 left, Int128 right) => new(left._upper ^ right._upper, left._lower ^ right._lower);

    public static Int128 operator ~(Int128 value) => new(~value._upper, ~value._lower);

    //
    // IComparisonOperators
    //

    public static bool operator <(Int128 left, Int128 right)
    {
        if (IsNegative(left) == IsNegative(right))
        {
            return (left._upper < right._upper)
                || ((left._upper == right._upper) && (left._lower < right._lower));
        }
        else
        {
            return IsNegative(left);
        }
    }

    public static bool operator <=(Int128 left, Int128 right)
    {
        if (IsNegative(left) == IsNegative(right))
        {
            return (left._upper < right._upper)
                || ((left._upper == right._upper) && (left._lower <= right._lower));
        }
        else
        {
            return IsNegative(left);
        }
    }

    public static bool operator >(Int128 left, Int128 right)
    {
        if (IsNegative(left) == IsNegative(right))
        {
            return (left._upper > right._upper)
                || ((left._upper == right._upper) && (left._lower > right._lower));
        }
        else
        {
            return IsNegative(right);
        }
    }

    public static bool operator >=(Int128 left, Int128 right)
    {
        if (IsNegative(left) == IsNegative(right))
        {
            return (left._upper > right._upper)
                || ((left._upper == right._upper) && (left._lower >= right._lower));
        }
        else
        {
            return IsNegative(right);
        }
    }

    //
    // IDecrementOperators
    //

    public static Int128 operator --(Int128 value) => value - One;

    public static Int128 operator checked --(Int128 value) => checked(value - One);

    //
    // IEqualityOperators
    //

    public static bool operator ==(Int128 left, Int128 right) => (left._lower == right._lower) && (left._upper == right._upper);

    public static bool operator !=(Int128 left, Int128 right) => (left._lower != right._lower) || (left._upper != right._upper);

    //
    // IIncrementOperators
    //

    public static Int128 operator ++(Int128 value) => value + One;

    public static Int128 operator checked ++(Int128 value) => checked(value + One);

    //
    // IMinMaxValue
    //

    public static Int128 MinValue => new(0x8000_0000_0000_0000, 0);

    public static Int128 MaxValue => new(0x7FFF_FFFF_FFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF);

    //
    // IMultiplyOperators
    //

    public static Int128 operator *(Int128 left, Int128 right)
    {
        // Multiplication is the same for signed and unsigned provided the "upper" bits aren't needed
        return (Int128)((UInt128)(left) * (UInt128)(right));
    }

    public static Int128 operator checked *(Int128 left, Int128 right)
    {
        Int128 upper = BigMul(left, right, out Int128 lower);

        if (((upper != 0) || (lower < 0)) && ((~upper != 0) || (lower >= 0)))
        {
            // The upper bits can safely be either Zero or AllBitsSet
            // where the former represents a positive value and the
            // latter a negative value.
            //
            // However, when the upper bits are Zero, we also need to
            // confirm the lower bits are positive, otherwise we have
            // a positive value greater than MaxValue and should throw
            //
            // Likewise, when the upper bits are AllBitsSet, we also
            // need to confirm the lower bits are negative, otherwise
            // we have a large negative value less than MinValue and
            // should throw.

            throw new OverflowException();
        }

        return lower;
    }

    internal static Int128 BigMul(Int128 left, Int128 right, out Int128 lower)
    {
        // This follows the same logic as is used in `long Math.BigMul(long, long, out long)`

        UInt128 upper = UInt128.BigMul((UInt128)(left), (UInt128)(right), out UInt128 ulower);
        lower = (Int128)(ulower);
        return (Int128)(upper) - ((left >> 127) & right) - ((right >> 127) & left);
    }

    //
    // INumber
    //

    public static Int128 Max(Int128 x, Int128 y) => (x >= y) ? x : y;

    public static Int128 Min(Int128 x, Int128 y) => (x <= y) ? x : y;

    //
    // INumberBase
    //

    public static Int128 One => new(0, 1);

    public static Int128 Zero => default;

    public static Int128 Abs(Int128 value)
    {
        if (IsNegative(value))
        {
            value = -value;

            if (IsNegative(value))
            {
                throw new OverflowException("Negating the minimum value of a twos complement number is invalid.");
            }
        }
        return value;
    }

    public static bool IsEvenInteger(Int128 value) => (value._lower & 1) == 0;

    public static bool IsNegative(Int128 value) => (long)value._upper < 0;

    public static bool IsOddInteger(Int128 value) => (value._lower & 1) != 0;

    public static bool IsPositive(Int128 value) => (long)value._upper >= 0;

    //
    // IShiftOperators
    //

    public static Int128 operator <<(Int128 value, int shiftAmount)
    {
        // C# automatically masks the shift amount for UInt64 to be 0x3F. So we
        // need to specially handle things if the 7th bit is set.

        shiftAmount &= 0x7F;

        if ((shiftAmount & 0x40) != 0)
        {
            // In the case it is set, we know the entire lower bits must be zero
            // and so the upper bits are just the lower shifted by the remaining
            // masked amount

            ulong upper = value._lower << shiftAmount;
            return new Int128(upper, 0);
        }
        else if (shiftAmount != 0)
        {
            // Otherwise we need to shift both upper and lower halves by the masked
            // amount and then or that with whatever bits were shifted "out" of lower

            ulong lower = value._lower << shiftAmount;
            ulong upper = (value._upper << shiftAmount) | (value._lower >> (64 - shiftAmount));

            return new Int128(upper, lower);
        }
        else
        {
            return value;
        }
    }

    public static Int128 operator >>(Int128 value, int shiftAmount)
    {
        // C# automatically masks the shift amount for UInt64 to be 0x3F. So we
        // need to specially handle things if the 7th bit is set.

        shiftAmount &= 0x7F;

        if ((shiftAmount & 0x40) != 0)
        {
            // In the case it is set, we know the entire upper bits must be the sign
            // and so the lower bits are just the upper shifted by the remaining
            // masked amount

            ulong lower = (ulong)((long)value._upper >> shiftAmount);
            ulong upper = (ulong)((long)value._upper >> 63);

            return new Int128(upper, lower);
        }
        else if (shiftAmount != 0)
        {
            // Otherwise we need to shift both upper and lower halves by the masked
            // amount and then or that with whatever bits were shifted "out" of upper

            ulong lower = (value._lower >> shiftAmount) | (value._upper << (64 - shiftAmount));
            ulong upper = (ulong)((long)value._upper >> shiftAmount);

            return new Int128(upper, lower);
        }
        else
        {
            return value;
        }
    }

    public static Int128 operator >>>(Int128 value, int shiftAmount)
    {
        // C# automatically masks the shift amount for UInt64 to be 0x3F. So we
        // need to specially handle things if the 7th bit is set.

        shiftAmount &= 0x7F;

        if ((shiftAmount & 0x40) != 0)
        {
            // In the case it is set, we know the entire upper bits must be zero
            // and so the lower bits are just the upper shifted by the remaining
            // masked amount

            ulong lower = value._upper >> shiftAmount;
            return new Int128(0, lower);
        }
        else if (shiftAmount != 0)
        {
            // Otherwise we need to shift both upper and lower halves by the masked
            // amount and then or that with whatever bits were shifted "out" of upper

            ulong lower = (value._lower >> shiftAmount) | (value._upper << (64 - shiftAmount));
            ulong upper = value._upper >> shiftAmount;

            return new Int128(upper, lower);
        }
        else
        {
            return value;
        }
    }

    //
    // ISignedNumber
    //

    public static Int128 NegativeOne => new(0xFFFF_FFFF_FFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF);

    //
    // ISubtractionOperators
    //

    public static Int128 operator -(Int128 left, Int128 right)
    {
        // For unsigned subtract, we can detect overflow by checking `(x - y) > x`
        // This gives us the borrow to subtract from upper to compute the correct result

        ulong lower = left._lower - right._lower;
        ulong borrow = (lower > left._lower) ? 1UL : 0UL;

        ulong upper = left._upper - right._upper - borrow;
        return new Int128(upper, lower);
    }

    public static Int128 operator checked -(Int128 left, Int128 right)
    {
        // For signed subtraction, we can detect overflow by checking if the sign of
        // both inputs are different and then if that differs from the sign of the
        // output.

        Int128 result = left - right;

        uint sign = (uint)(left._upper >> 63);

        if (sign != (uint)(right._upper >> 63))
        {
            if (sign != (uint)(result._upper >> 63))
            {
                throw new OverflowException();
            }
        }
        return result;
    }

    //
    // IUnaryNegationOperators
    //

    public static Int128 operator -(Int128 value) => Zero - value;

    public static Int128 operator checked -(Int128 value) => checked(Zero - value);

    //
    // IUnaryPlusOperators
    //

    public static Int128 operator +(Int128 value) => value;
}

#endif
