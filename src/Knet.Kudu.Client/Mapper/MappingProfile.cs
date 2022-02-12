using System;
using System.Reflection;

namespace Knet.Kudu.Client.Mapper;

internal sealed record MappingProfile(
    ConstructorInfo Constructor,
    MappingConstructorParameter[] ConstructorParameters,
    MappingProperty[] Properties);

internal readonly record struct MappingConstructorParameter(
    int ColumnIndex,
    Type DestinationType,
    MethodInfo ResultSetMethod);

internal readonly record struct MappingProperty(
    int ColumnIndex,
    PropertyInfo PropertyInfo,
    MethodInfo ResultSetMethod);
