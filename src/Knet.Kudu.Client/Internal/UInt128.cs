// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#if !NET7_0_OR_GREATER

using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Knet.Kudu.Client.Internal;

/// <summary>Represents a 128-bit unsigned integer.</summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct UInt128 : IEquatable<UInt128>, IComparable<UInt128>
{
    internal const int Size = 16;

    private readonly ulong _lower;
    private readonly ulong _upper;

    /// <summary>Initializes a new instance of the <see cref="UInt128" /> struct.</summary>
    /// <param name="upper">The upper 64-bits of the 128-bit value.</param>
    /// <param name="lower">The lower 64-bits of the 128-bit value.</param>
    public UInt128(ulong upper, ulong lower)
    {
        _lower = lower;
        _upper = upper;
    }

    internal ulong Lower => _lower;

    internal ulong Upper => _upper;

    /// <inheritdoc cref="IComparable.CompareTo(object)" />
    public int CompareTo(object? value)
    {
        if (value is UInt128 other)
        {
            return CompareTo(other);
        }
        else if (value is null)
        {
            return 1;
        }
        else
        {
            throw new ArgumentException("Object must be of type UInt128.");
        }
    }

    /// <inheritdoc cref="IComparable{T}.CompareTo(T)" />
    public int CompareTo(UInt128 value)
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
        return (obj is UInt128 other) && Equals(other);
    }

    /// <inheritdoc cref="IEquatable{T}.Equals(T)" />
    public bool Equals(UInt128 other)
    {
        return this == other;
    }

    /// <inheritdoc cref="object.GetHashCode()" />
    public override int GetHashCode() => HashCode.Combine(_lower, _upper);

    //
    // Explicit Conversions From UInt128
    //

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="byte" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="byte" />.</returns>
    public static explicit operator byte(UInt128 value) => (byte)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="byte" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="byte" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked byte(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((byte)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="char" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="char" />.</returns>
    public static explicit operator char(UInt128 value) => (char)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="char" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="char" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked char(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((char)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="decimal" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="decimal" />.</returns>
    public static explicit operator decimal(UInt128 value)
    {
        ulong lo64 = value._lower;

        if (value._upper > uint.MaxValue)
        {
            // The default behavior of decimal conversions is to always throw on overflow
            throw new OverflowException();
        }

        uint hi32 = (uint)(value._upper);

        return new decimal((int)(lo64), (int)(lo64 >> 32), (int)(hi32), isNegative: false, scale: 0);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="short" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="short" />.</returns>
    public static explicit operator short(UInt128 value) => (short)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="short" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="short" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked short(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((short)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="int" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="int" />.</returns>
    public static explicit operator int(UInt128 value) => (int)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="int" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="int" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked int(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((int)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="long" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="long" />.</returns>
    public static explicit operator long(UInt128 value) => (long)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="long" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="long" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked long(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((long)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="Int128" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="Int128" />.</returns>
    public static explicit operator Int128(UInt128 value) => new(value._upper, value._lower);

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="Int128" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="Int128" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked Int128(UInt128 value)
    {
        if ((long)value._upper < 0)
        {
            throw new OverflowException();
        }
        return new Int128(value._upper, value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="IntPtr" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="IntPtr" />.</returns>
    public static explicit operator nint(UInt128 value) => (nint)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="IntPtr" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="IntPtr" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked nint(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((nint)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="sbyte" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="sbyte" />.</returns>
    public static explicit operator sbyte(UInt128 value) => (sbyte)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="sbyte" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="sbyte" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked sbyte(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((sbyte)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="float" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="float" />.</returns>
    public static explicit operator float(UInt128 value) => (float)(double)(value);

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="ushort" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ushort" />.</returns>
    public static explicit operator ushort(UInt128 value) => (ushort)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="ushort" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ushort" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked ushort(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((ushort)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="uint" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="uint" />.</returns>
    public static explicit operator uint(UInt128 value) => (uint)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="uint" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="uint" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked uint(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((uint)value._lower);
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="ulong" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ulong" />.</returns>
    public static explicit operator ulong(UInt128 value) => value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="ulong" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="ulong" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked ulong(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return value._lower;
    }

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="UIntPtr" /> value.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="UIntPtr" />.</returns>
    public static explicit operator nuint(UInt128 value) => (nuint)value._lower;

    /// <summary>Explicitly converts a 128-bit unsigned integer to a <see cref="UIntPtr" /> value, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a <see cref="UIntPtr" />.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked nuint(UInt128 value)
    {
        if (value._upper != 0)
        {
            throw new OverflowException();
        }
        return checked((nuint)value._lower);
    }

    //
    // Explicit Conversions To UInt128
    //

    /// <summary>Explicitly converts a <see cref="short" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static explicit operator UInt128(short value)
    {
        long lower = value;
        return new UInt128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Explicitly converts a <see cref="short" /> value to a 128-bit unsigned integer, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked UInt128(short value)
    {
        if (value < 0)
        {
            throw new OverflowException();
        }
        return new UInt128(0, (ushort)value);
    }

    /// <summary>Explicitly converts a <see cref="int" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static explicit operator UInt128(int value)
    {
        long lower = value;
        return new UInt128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Explicitly converts a <see cref="int" /> value to a 128-bit unsigned integer, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked UInt128(int value)
    {
        if (value < 0)
        {
            throw new OverflowException();
        }
        return new UInt128(0, (uint)value);
    }

    /// <summary>Explicitly converts a <see cref="long" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static explicit operator UInt128(long value)
    {
        long lower = value;
        return new UInt128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Explicitly converts a <see cref="long" /> value to a 128-bit unsigned integer, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked UInt128(long value)
    {
        if (value < 0)
        {
            throw new OverflowException();
        }
        return new UInt128(0, (ulong)value);
    }

    /// <summary>Explicitly converts a <see cref="IntPtr" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static explicit operator UInt128(nint value)
    {
        long lower = value;
        return new UInt128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Explicitly converts a <see cref="IntPtr" /> value to a 128-bit unsigned integer, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked UInt128(nint value)
    {
        if (value < 0)
        {
            throw new OverflowException();
        }
        return new UInt128(0, (nuint)value);
    }

    /// <summary>Explicitly converts a <see cref="sbyte" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static explicit operator UInt128(sbyte value)
    {
        long lower = value;
        return new UInt128((ulong)(lower >> 63), (ulong)lower);
    }

    /// <summary>Explicitly converts a <see cref="sbyte" /> value to a 128-bit unsigned integer, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked UInt128(sbyte value)
    {
        if (value < 0)
        {
            throw new OverflowException();
        }
        return new UInt128(0, (byte)value);
    }

    /// <summary>Explicitly converts a <see cref="float" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static explicit operator UInt128(float value) => (UInt128)(double)(value);

    /// <summary>Explicitly converts a <see cref="float" /> value to a 128-bit unsigned integer, throwing an overflow exception for any values that fall outside the representable range.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    /// <exception cref="OverflowException"><paramref name="value" /> is not representable by <see cref="UInt128" />.</exception>
    public static explicit operator checked UInt128(float value) => checked((UInt128)(double)(value));

    //
    // Implicit Conversions To UInt128
    //

    /// <summary>Implicitly converts a <see cref="byte" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static implicit operator UInt128(byte value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="char" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static implicit operator UInt128(char value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="ushort" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static implicit operator UInt128(ushort value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="uint" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static implicit operator UInt128(uint value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="ulong" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static implicit operator UInt128(ulong value) => new(0, value);

    /// <summary>Implicitly converts a <see cref="UIntPtr" /> value to a 128-bit unsigned integer.</summary>
    /// <param name="value">The value to convert.</param>
    /// <returns><paramref name="value" /> converted to a 128-bit unsigned integer.</returns>
    public static implicit operator UInt128(nuint value) => new(0, value);

    //
    // IAdditionOperators
    //

    public static UInt128 operator +(UInt128 left, UInt128 right)
    {
        // For unsigned addition, we can detect overflow by checking `(x + y) < x`
        // This gives us the carry to add to upper to compute the correct result

        ulong lower = left._lower + right._lower;
        ulong carry = (lower < left._lower) ? 1UL : 0UL;

        ulong upper = left._upper + right._upper + carry;
        return new UInt128(upper, lower);
    }

    public static UInt128 operator checked +(UInt128 left, UInt128 right)
    {
        // For unsigned addition, we can detect overflow by checking `(x + y) < x`
        // This gives us the carry to add to upper to compute the correct result

        ulong lower = left._lower + right._lower;
        ulong carry = (lower < left._lower) ? 1UL : 0UL;

        ulong upper = checked(left._upper + right._upper + carry);
        return new UInt128(upper, lower);
    }

    //
    // IBinaryInteger
    //

    public static UInt128 RotateLeft(UInt128 value, int rotateAmount)
        => (value << rotateAmount) | (value >>> (128 - rotateAmount));

    public static UInt128 RotateRight(UInt128 value, int rotateAmount)
        => (value >>> rotateAmount) | (value << (128 - rotateAmount));

    //
    // IBitwiseOperators
    //

    public static UInt128 operator &(UInt128 left, UInt128 right) => new(left._upper & right._upper, left._lower & right._lower);

    public static UInt128 operator |(UInt128 left, UInt128 right) => new(left._upper | right._upper, left._lower | right._lower);

    public static UInt128 operator ^(UInt128 left, UInt128 right) => new(left._upper ^ right._upper, left._lower ^ right._lower);

    public static UInt128 operator ~(UInt128 value) => new(~value._upper, ~value._lower);

    //
    // IComparisonOperators
    //

    public static bool operator <(UInt128 left, UInt128 right)
    {
        return (left._upper < right._upper)
            || (left._upper == right._upper) && (left._lower < right._lower);
    }

    public static bool operator <=(UInt128 left, UInt128 right)
    {
        return (left._upper < right._upper)
            || (left._upper == right._upper) && (left._lower <= right._lower);
    }

    public static bool operator >(UInt128 left, UInt128 right)
    {
        return (left._upper > right._upper)
            || (left._upper == right._upper) && (left._lower > right._lower);
    }

    public static bool operator >=(UInt128 left, UInt128 right)
    {
        return (left._upper > right._upper)
            || (left._upper == right._upper) && (left._lower >= right._lower);
    }

    //
    // IDecrementOperators
    //

    public static UInt128 operator --(UInt128 value) => value - One;

    public static UInt128 operator checked --(UInt128 value) => checked(value - One);

    //
    // IEqualityOperators
    //

    public static bool operator ==(UInt128 left, UInt128 right) => (left._lower == right._lower) && (left._upper == right._upper);

    public static bool operator !=(UInt128 left, UInt128 right) => (left._lower != right._lower) || (left._upper != right._upper);

    //
    // IIncrementOperators
    //

    public static UInt128 operator ++(UInt128 value) => value + One;

    public static UInt128 operator checked ++(UInt128 value) => checked(value + One);

    //
    // IMinMaxValue
    //

    public static UInt128 MinValue => new(0, 0);

    public static UInt128 MaxValue => new(0xFFFF_FFFF_FFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF);

    //
    // IMultiplyOperators
    //

    public static UInt128 operator *(UInt128 left, UInt128 right)
    {
        ulong upper = BigMul(left._lower, right._lower, out ulong lower);
        upper += (left._upper * right._lower) + (left._lower * right._upper);
        return new UInt128(upper, lower);

        static ulong BigMul(ulong a, ulong b, out ulong low)
        {
            // Adaptation of algorithm for multiplication
            // of 32-bit unsigned integers described
            // in Hacker's Delight by Henry S. Warren, Jr. (ISBN 0-201-91465-4), Chapter 8
            // Basically, it's an optimized version of FOIL method applied to
            // low and high dwords of each operand

            // Use 32-bit uints to optimize the fallback for 32-bit platforms.
            uint al = (uint)a;
            uint ah = (uint)(a >> 32);
            uint bl = (uint)b;
            uint bh = (uint)(b >> 32);

            ulong mull = ((ulong)al) * bl;
            ulong t = ((ulong)ah) * bl + (mull >> 32);
            ulong tl = ((ulong)al) * bh + (uint)t;

            low = tl << 32 | (uint)mull;

            return ((ulong)ah) * bh + (t >> 32) + (tl >> 32);
        }
    }

    public static UInt128 operator checked *(UInt128 left, UInt128 right)
    {
        UInt128 upper = BigMul(left, right, out UInt128 lower);

        if (upper != 0U)
        {
            throw new OverflowException();
        }

        return lower;
    }

    internal static UInt128 BigMul(UInt128 left, UInt128 right, out UInt128 lower)
    {
        // Adaptation of algorithm for multiplication
        // of 32-bit unsigned integers described
        // in Hacker's Delight by Henry S. Warren, Jr. (ISBN 0-201-91465-4), Chapter 8
        // Basically, it's an optimized version of FOIL method applied to
        // low and high qwords of each operand

        UInt128 al = left._lower;
        UInt128 ah = left._upper;

        UInt128 bl = right._lower;
        UInt128 bh = right._upper;

        UInt128 mull = al * bl;
        UInt128 t = ah * bl + mull._upper;
        UInt128 tl = al * bh + t._lower;

        lower = new UInt128(tl._lower, mull._lower);
        return ah * bh + t._upper + tl._upper;
    }

    //
    // INumber
    //

    public static UInt128 Max(UInt128 x, UInt128 y) => (x >= y) ? x : y;

    public static UInt128 Min(UInt128 x, UInt128 y) => (x <= y) ? x : y;

    //
    // INumberBase
    //

    public static UInt128 One => new(0, 1);

    public static UInt128 Zero => default;

    public static bool IsEvenInteger(UInt128 value) => (value._lower & 1) == 0;

    //
    // IShiftOperators
    //

    public static UInt128 operator <<(UInt128 value, int shiftAmount)
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
            return new UInt128(upper, 0);
        }
        else if (shiftAmount != 0)
        {
            // Otherwise we need to shift both upper and lower halves by the masked
            // amount and then or that with whatever bits were shifted "out" of lower

            ulong lower = value._lower << shiftAmount;
            ulong upper = (value._upper << shiftAmount) | (value._lower >> (64 - shiftAmount));

            return new UInt128(upper, lower);
        }
        else
        {
            return value;
        }
    }

    public static UInt128 operator >>(UInt128 value, int shiftAmount) => value >>> shiftAmount;

    public static UInt128 operator >>>(UInt128 value, int shiftAmount)
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
            return new UInt128(0, lower);
        }
        else if (shiftAmount != 0)
        {
            // Otherwise we need to shift both upper and lower halves by the masked
            // amount and then or that with whatever bits were shifted "out" of upper

            ulong lower = (value._lower >> shiftAmount) | (value._upper << (64 - shiftAmount));
            ulong upper = value._upper >> shiftAmount;

            return new UInt128(upper, lower);
        }
        else
        {
            return value;
        }
    }

    //
    // ISubtractionOperators
    //

    public static UInt128 operator -(UInt128 left, UInt128 right)
    {
        // For unsigned subtract, we can detect overflow by checking `(x - y) > x`
        // This gives us the borrow to subtract from upper to compute the correct result

        ulong lower = left._lower - right._lower;
        ulong borrow = (lower > left._lower) ? 1UL : 0UL;

        ulong upper = left._upper - right._upper - borrow;
        return new UInt128(upper, lower);
    }

    public static UInt128 operator checked -(UInt128 left, UInt128 right)
    {
        // For unsigned subtract, we can detect overflow by checking `(x - y) > x`
        // This gives us the borrow to subtract from upper to compute the correct result

        ulong lower = left._lower - right._lower;
        ulong borrow = (lower > left._lower) ? 1UL : 0UL;

        ulong upper = checked(left._upper - right._upper - borrow);
        return new UInt128(upper, lower);
    }

    //
    // IUnaryNegationOperators
    //

    public static UInt128 operator -(UInt128 value) => Zero - value;

    public static UInt128 operator checked -(UInt128 value) => checked(Zero - value);

    //
    // IUnaryPlusOperators
    //

    public static UInt128 operator +(UInt128 value) => value;
}

#endif
