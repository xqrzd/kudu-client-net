using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
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
        { KuduType.UnixtimeMicros, new[] { typeof(DateTime), typeof(long) } },
        { KuduType.Date, new[] { typeof(DateTime), typeof(int), typeof(long) } },
        { KuduType.String, new[] { typeof(string) } },
        { KuduType.Varchar, new[] { typeof(string) } },
        { KuduType.Binary, new[] { typeof(byte[]), typeof(ReadOnlyMemory<byte>) } },
        { KuduType.Decimal32, new[] { typeof(decimal) } },
        { KuduType.Decimal64, new[] { typeof(decimal) } },
        { KuduType.Decimal128, new[] { typeof(decimal) } }
    };

    private static readonly Dictionary<string, MethodInfo> _resultSetMethods = GetResultSetMethods();

    public static Func<ResultSet, int, T> Create<T>(KuduSchema projectionSchema)
    {
        if (projectionSchema.Columns.Count == 0)
        {
            throw new ArgumentException(
                "No columns were projected for this scan, use CountAsync() instead");
        }

        var destinationType = typeof(T);
        var resultSet = Expression.Parameter(typeof(ResultSet), "resultSet");
        var rowIndex = Expression.Parameter(typeof(int), "rowIndex");

        var body = CreateMappingExpression(projectionSchema, destinationType, resultSet, rowIndex);

        var lambda = Expression.Lambda<Func<ResultSet, int, T>>(body, resultSet, rowIndex);
        var func = lambda.Compile();

        return func;
    }

    private static Expression CreateMappingExpression(
        KuduSchema schema,
        Type destinationType,
        ParameterExpression resultSet,
        ParameterExpression rowIndex)
    {
        if (schema.Columns.Count == 1)
        {
            var column = schema.Columns[0];
            if (TryGetResultSetMethod(column.Type, column.IsNullable, destinationType, out var method))
            {
                return CreateValueExpression(resultSet, rowIndex, method, 0, destinationType);
            }
        }

        var constructor = GetConstructor(schema, destinationType);
        var properties = GetMappedProperties(schema, destinationType, constructor.Parameters);

        var returnVal = Expression.Variable(destinationType, "result");
        var constructorExpression = CreateConstructorExpression(constructor, resultSet, rowIndex);

        var expressions = new List<Expression>(properties.Length + 2)
        {
            Expression.Assign(returnVal, constructorExpression)
        };

        CreatePropertyExpressions(properties, resultSet, rowIndex, returnVal, expressions);

        expressions.Add(returnVal);

        return Expression.Block(destinationType, new[] { returnVal }, expressions);
    }

    private static MappedConstructor GetConstructor(KuduSchema schema, Type destinationType)
    {
        var constructors = destinationType.GetConstructors();

        if (IsValueTuple(destinationType) && constructors.Length == 1)
        {
            return GetValueTupleConstructor(schema, constructors[0]);
        }

        var projectedColumns = schema.Columns
            .Select((column, index) => new ColumnInfo(index, column.Name, column.Type, column.IsNullable));

        var columnMatcher = new ColumnNameMatcher<ColumnInfo>(projectedColumns, column => column.ColumnName);

        MappedConstructor mappedConstructor = default;

        foreach (var constructor in constructors)
        {
            if (TryMatchConstructor(constructor, columnMatcher, out var parameters))
            {
                // Pick the constructor with the most parameters.
                if (mappedConstructor.Parameters is null ||
                    parameters.Length > mappedConstructor.Parameters.Length)
                {
                    mappedConstructor = new MappedConstructor(constructor, parameters);
                }
            }
        }

        if (mappedConstructor.ConstructorInfo is null)
        {
            throw new ArgumentException(
                "No parameterless constructor or one matching projected types was found.");
        }

        return mappedConstructor;
    }

    private static MappedConstructor GetValueTupleConstructor(KuduSchema schema, ConstructorInfo constructor)
    {
        var columns = schema.Columns;
        var parameters = constructor.GetParameters();

        if (columns.Count != parameters.Length)
        {
            throw new ArgumentException(
                $"ValueTuple has {parameters.Length} properties, " +
                $"but projection schema has {columns.Count} columns");
        }

        var results = new ConstructorParameter[parameters.Length];

        for (int i = 0; i < parameters.Length; i++)
        {
            var column = columns[i];
            var parameter = parameters[i];
            var parameterType = parameter.ParameterType;

            if (TryGetResultSetMethod(column.Type, column.IsNullable, parameterType, out var method))
            {
                results[i] = new ConstructorParameter(i, parameterType, method);
            }
            else
            {
                throw new ArgumentException(
                    $"Column `{column}` cannot be converted to ValueTuple property `{parameter}`");
            }
        }

        return new MappedConstructor(constructor, results);
    }

    private static bool TryMatchConstructor(
        ConstructorInfo constructor,
        ColumnNameMatcher<ColumnInfo> columnMatcher,
        [NotNullWhen(true)] out ConstructorParameter[]? mappedParameters)
    {
        var parameters = constructor.GetParameters();
        var results = new ConstructorParameter[parameters.Length];
        int i = 0;

        foreach (var parameter in parameters)
        {
            var parameterName = parameter.Name;
            var parameterType = parameter.ParameterType;

            if (parameterName is not null &&
                columnMatcher.TryGetColumn(parameterName, out var columnInfo) &&
                TryGetResultSetMethod(columnInfo.KuduType, columnInfo.IsNullable, parameterType, out var method))
            {
                results[i++] = new ConstructorParameter(
                    columnInfo.ColumnIndex, parameterType, method);
            }
            else
            {
                // There isn't a projected column to use for this parameter.
                mappedParameters = null;
                return false;
            }
        }

        mappedParameters = results;
        return true;
    }

    private static MappedProperty[] GetMappedProperties(
        KuduSchema schema,
        Type destinationType,
        ConstructorParameter[] constructorParameters)
    {
        var constructorColumnIndexes = new HashSet<int>();
        foreach (var parameter in constructorParameters)
        {
            constructorColumnIndexes.Add(parameter.ColumnIndex);
        }

        var destinationProperties = destinationType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.SetMethod is not null);

        var columnMatcher = new ColumnNameMatcher<PropertyInfo>(
            destinationProperties, column => column.Name);

        var columns = schema.Columns;
        var numColumns = columns.Count;

        var results = new MappedProperty[numColumns - constructorParameters.Length];
        int resultIndex = 0;

        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++)
        {
            if (constructorColumnIndexes.Contains(columnIndex))
            {
                // This column is passed into the constructor.
                continue;
            }

            var column = columns[columnIndex];

            if (columnMatcher.TryGetColumn(column.Name, out var propertyInfo) &&
                TryGetResultSetMethod(column.Type, column.IsNullable, propertyInfo.PropertyType, out var method))
            {
                results[resultIndex++] = new MappedProperty(columnIndex, propertyInfo, method);
            }
            else
            {
                throw new ArgumentException(
                    $"Column {column} does not match a constructor parameter or property");
            }
        }

        return results;
    }

    private static Expression CreateConstructorExpression(
        MappedConstructor constructor,
        ParameterExpression resultSet,
        ParameterExpression rowIndex)
    {
        var parameters = constructor.Parameters;
        var arguments = new Expression[parameters.Length];

        for (int i = 0; i < arguments.Length; i++)
        {
            var parameter = parameters[i];

            var expression = CreateValueExpression(
                resultSet,
                rowIndex,
                parameter.ResultSetMethod,
                parameter.ColumnIndex,
                parameter.DestinationType);

            arguments[i] = expression;
        }

        return Expression.New(constructor.ConstructorInfo, arguments);
    }

    private static void CreatePropertyExpressions(
        MappedProperty[] properties,
        ParameterExpression resultSet,
        ParameterExpression rowIndex,
        ParameterExpression returnVal,
        List<Expression> output)
    {
        foreach (var property in properties)
        {
            var propertyInfo = property.PropertyInfo;

            var getValueExpression = CreateValueExpression(
                resultSet,
                rowIndex,
                property.ResultSetMethod,
                property.ColumnIndex,
                propertyInfo.PropertyType);

            // returnVal.Property = resultSet.ReadInt32(...);
            var expression = Expression.Assign(
                Expression.Property(returnVal, propertyInfo),
                getValueExpression);

            output.Add(expression);
        }
    }

    private static Expression CreateValueExpression(
        ParameterExpression resultSet,
        ParameterExpression rowIndex,
        MethodInfo method,
        int columnIndex,
        Type destinationType)
    {
        var expression = Expression.Call(
            resultSet,
            method,
            Expression.Constant(columnIndex),
            rowIndex);

        if (destinationType != method.ReturnType)
        {
            return Expression.Convert(expression, destinationType);
        }

        return expression;
    }

    private static bool TryGetResultSetMethod(
        KuduType columnType,
        bool isNullable,
        Type destinationType,
        [NotNullWhen(true)] out MethodInfo? method)
    {
        var underlyingType = GetUnderlyingType(destinationType);

        if (_allowedConversions.TryGetValue(columnType, out var allowedTypes) &&
            allowedTypes.Contains(underlyingType))
        {
            method = GetResultSetMethod(columnType, isNullable, underlyingType);
            return true;
        }

        method = null;
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

    private static bool IsValueTuple(Type type)
    {
        var underlyingType = GetUnderlyingType(type);

        return
            underlyingType.IsValueType &&
            underlyingType.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true;
    }

    private static MethodInfo GetResultSetMethod(KuduType columnType, bool isNullable, Type underlyingType)
    {
        var methods = _resultSetMethods;

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
            (KuduType.Binary, false) => methods[nameof(ResultSet.GetBinary)],
            (KuduType.Binary, true) => methods[nameof(ResultSet.GetNullableBinary)],
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

    private readonly record struct MappedConstructor(
        ConstructorInfo ConstructorInfo,
        ConstructorParameter[] Parameters);

    private readonly record struct ConstructorParameter(
        int ColumnIndex,
        Type DestinationType,
        MethodInfo ResultSetMethod);

    private readonly record struct MappedProperty(
        int ColumnIndex,
        PropertyInfo PropertyInfo,
        MethodInfo ResultSetMethod);

    private sealed record ColumnInfo(
        int ColumnIndex,
        string ColumnName,
        KuduType KuduType,
        bool IsNullable);
}
