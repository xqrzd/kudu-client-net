using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;

namespace Knet.Kudu.Client.Mapper;

internal static class MappingProfileFactory
{
    // https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/language-specification/conversions
    private static readonly Dictionary<KuduType, Type[]> _allowedConversions = new()
    {
        { KuduType.Bool, new[] { typeof(bool) } },
        { KuduType.Int8, new[] { typeof(sbyte), typeof(short), typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal) } },
        { KuduType.Int16, new[] { typeof(short), typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal) } },
        { KuduType.Int32, new[] { typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal) } },
        { KuduType.Int64, new[] { typeof(long), typeof(float), typeof(double), typeof(decimal) } },
        { KuduType.Float, new[] { typeof(float), typeof(double) } },
        { KuduType.Double, new[] { typeof(double) } },
        { KuduType.UnixtimeMicros, new[] { typeof(long), typeof(DateTime) } },
        { KuduType.Date, new[] { typeof(int), typeof(DateTime) } },
        { KuduType.String, new[] { typeof(string) } },
        { KuduType.Varchar, new[] { typeof(string) } },
        { KuduType.Binary, new[] { typeof(byte[]) } },
        { KuduType.Decimal32, new[] { typeof(decimal) } },
        { KuduType.Decimal64, new[] { typeof(decimal) } },
        { KuduType.Decimal128, new[] { typeof(decimal) } }
    };

    private static readonly Dictionary<string, MethodInfo> _resultSetMethods = GetResultSetMethods();

    public static MappingProfile Create(KuduSchema schema, Type destinationType)
    {
        var constructor = GetConstructor(schema, destinationType);
        var properties = GetProperties(schema, destinationType, constructor.Parameters);

        return new MappingProfile(constructor.Constructor, constructor.Parameters, properties);
    }

    private static MatchingConstructor GetConstructor(KuduSchema schema, Type destinationType)
    {
        var projectedColumns = CreateSchemaLookup(schema);
        var constructors = destinationType.GetConstructors();
        MatchingConstructor matchingConstructor = default;

        foreach (var constructor in constructors)
        {
            if (TryMatchConstructor(constructor, projectedColumns, out var parameters))
            {
                // Pick the constructor with the most parameters.
                if (matchingConstructor.Parameters is null ||
                    parameters.Length > matchingConstructor.Parameters.Length)
                {
                    matchingConstructor = new MatchingConstructor(constructor, parameters);
                }
            }
        }

        if (matchingConstructor.Constructor is null)
        {
            // No parameterless constructor was found, or one matching projected types.
            // TODO: Try to find an exact one (eg. ValueTuple)
            throw new Exception("No constructor was found");
        }

        return matchingConstructor;
    }

    private static bool TryMatchConstructor(
        ConstructorInfo constructor,
        Dictionary<string, ColumnInfo> projectedColumns,
        [NotNullWhen(true)] out MappingConstructorParameter[]? mappingParameters)
    {
        var parameters = constructor.GetParameters();
        var results = new MappingConstructorParameter[parameters.Length];
        int i = 0;

        foreach (var parameter in parameters)
        {
            var parameterName = parameter.Name;
            var parameterType = parameter.ParameterType;

            if (parameterName is not null &&
                projectedColumns.TryGetValue(parameterName, out var columnInfo) &&
                IsValidDestination(columnInfo.KuduType, parameterType))
            {
                var method = GetResultSetMethod(
                    columnInfo.KuduType,
                    columnInfo.IsNullable,
                    parameterType);

                results[i++] = new MappingConstructorParameter(
                    columnInfo.ColumnIndex,
                    parameterType,
                    method);
            }
            else
            {
                // There isn't a projected column to use for this parameter.
                mappingParameters = null;
                return false;
            }
        }

        mappingParameters = results;
        return true;
    }

    private static MappingProperty[] GetProperties(
        KuduSchema schema,
        Type destinationType,
        MappingConstructorParameter[] constructorParameters)
    {
        var constructorColumnIndexes = new HashSet<int>();
        foreach (var parameter in constructorParameters)
        {
            constructorColumnIndexes.Add(parameter.ColumnIndex);
        }

        var destinationProperties = destinationType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.SetMethod is not null)
            .ToDictionary(property => property.Name, StringComparer.OrdinalIgnoreCase);

        var columns = schema.Columns;
        var numColumns = columns.Count;

        var results = new MappingProperty[numColumns - constructorParameters.Length];
        int resultIndex = 0;

        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++)
        {
            if (constructorColumnIndexes.Contains(columnIndex))
            {
                // This column is passed into the constructor.
                continue;
            }

            var column = columns[columnIndex];

            if (destinationProperties.TryGetValue(column.Name, out var propertyInfo) &&
                IsValidDestination(column.Type, propertyInfo.PropertyType))
            {
                var method = GetResultSetMethod(
                    column.Type,
                    column.IsNullable,
                    propertyInfo.PropertyType);

                results[resultIndex++] = new MappingProperty(
                    columnIndex,
                    propertyInfo,
                    method);
            }
            else
            {
                throw new ArgumentException(
                    $"Column {column} does not match a constructor parameter or property");
            }
        }

        return results;
    }

    private static Dictionary<string, ColumnInfo> CreateSchemaLookup(KuduSchema schema)
    {
        var lookup = new Dictionary<string, ColumnInfo>(
            schema.Columns.Count,
            StringComparer.OrdinalIgnoreCase);

        int i = 0;

        foreach (var column in schema.Columns)
        {
            var columnInfo = new ColumnInfo(i++, column.Type, column.IsNullable);
            lookup.Add(column.Name, columnInfo);
        }

        return lookup;
    }

    private static bool IsValidDestination(KuduType columnType, Type destinationType)
    {
        var underlyingType = GetUnderlyingType(destinationType);

        if (_allowedConversions.TryGetValue(columnType, out var allowedTypes))
        {
            return allowedTypes.Contains(underlyingType);
        }

        return false;
    }

    private static Type GetUnderlyingType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType is not null)
        {
            // Recursively call GetUnderlyingType()
            // in case we have a nullable enum.
            return GetUnderlyingType(underlyingType);
        }

        if (type.IsEnum)
        {
            return Enum.GetUnderlyingType(type);
        }

        return type;
    }

    private static MethodInfo GetResultSetMethod(KuduType columnType, bool isNullable, Type destinationType)
    {
        var methods = _resultSetMethods;
        var underlyingType = GetUnderlyingType(destinationType);

        return (columnType, isNullable) switch
        {
            (KuduType.Bool, false) => methods[nameof(ResultSet.GetBool)],
            (KuduType.Bool, true) => methods[nameof(ResultSet.GetNullableBool)],
            (KuduType.Int8, false) => methods[nameof(ResultSet.GetSByte)],
            (KuduType.Int8, true) => methods[nameof(ResultSet.GetNullableSByte)],
            (KuduType.Int16, false) => methods[nameof(ResultSet.GetInt16)],
            (KuduType.Int16, true) => methods[nameof(ResultSet.GetNullableInt16)],
            (KuduType.Int32, false) => methods[nameof(ResultSet.GetInt32)],
            (KuduType.Int32, true) => methods[nameof(ResultSet.GetNullableInt32)],
            (KuduType.Int64, false) => methods[nameof(ResultSet.GetInt64)],
            (KuduType.Int64, true) => methods[nameof(ResultSet.GetNullableInt64)],
            (KuduType.Float, false) => methods[nameof(ResultSet.GetFloat)],
            (KuduType.Float, true) => methods[nameof(ResultSet.GetNullableFloat)],
            (KuduType.Double, false) => methods[nameof(ResultSet.GetDouble)],
            (KuduType.Double, true) => methods[nameof(ResultSet.GetNullableDouble)],
            (KuduType.UnixtimeMicros, false) when underlyingType == typeof(DateTime) => methods[nameof(ResultSet.GetDateTime)],
            (KuduType.UnixtimeMicros, true) when underlyingType == typeof(DateTime) => methods[nameof(ResultSet.GetNullableDateTime)],
            (KuduType.UnixtimeMicros, false) => methods[nameof(ResultSet.GetInt64)],
            (KuduType.UnixtimeMicros, true) => methods[nameof(ResultSet.GetNullableInt64)],
            (KuduType.Date, false) when underlyingType == typeof(DateTime) => methods[nameof(ResultSet.GetDateTime)],
            (KuduType.Date, true) when underlyingType == typeof(DateTime) => methods[nameof(ResultSet.GetNullableDateTime)],
            (KuduType.Date, false) => methods[nameof(ResultSet.GetInt32)],
            (KuduType.Date, true) => methods[nameof(ResultSet.GetNullableInt32)],
            (KuduType.String, false) => methods[nameof(ResultSet.GetString)],
            (KuduType.String, true) => methods[nameof(ResultSet.GetNullableString)],
            (KuduType.Varchar, false) => methods[nameof(ResultSet.GetString)],
            (KuduType.Varchar, true) => methods[nameof(ResultSet.GetNullableString)],
            // TODO: Binary
            (KuduType.Decimal32, false) => methods[nameof(ResultSet.GetDecimal)],
            (KuduType.Decimal32, true) => methods[nameof(ResultSet.GetNullableDecimal)],
            (KuduType.Decimal64, false) => methods[nameof(ResultSet.GetDecimal)],
            (KuduType.Decimal64, true) => methods[nameof(ResultSet.GetNullableDecimal)],
            (KuduType.Decimal128, false) => methods[nameof(ResultSet.GetDecimal)],
            (KuduType.Decimal128, true) => methods[nameof(ResultSet.GetNullableDecimal)],
            _ => throw new NotImplementedException()
        };
    }

    private static Dictionary<string, MethodInfo> GetResultSetMethods()
    {
        var methods = typeof(ResultSet).GetRuntimeMethods();
        var results = new Dictionary<string, MethodInfo>();

        foreach (var method in methods)
        {
            var parameters = method.GetParameters();

            if (method.Name.StartsWith("Get", StringComparison.Ordinal) &&
                !method.Name.EndsWith("Unsafe", StringComparison.Ordinal) &&
                parameters.Length == 2 &&
                parameters[0].ParameterType == typeof(int) &&
                parameters[1].ParameterType == typeof(int))
            {
                results.Add(method.Name, method);
            }
        }

        return results;
    }

    private readonly record struct MatchingConstructor(
        ConstructorInfo Constructor,
        MappingConstructorParameter[] Parameters);

    private readonly record struct ColumnInfo(int ColumnIndex, KuduType KuduType, bool IsNullable);
}
