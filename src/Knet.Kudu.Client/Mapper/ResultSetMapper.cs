using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Knet.Kudu.Client.Mapper
{
    public sealed class ResultSetMapper : IResultSetMapper
    {
        private readonly DelegateCache _cache = new();

        public Func<ResultSet, int, T> CreateDelegate<T>(KuduSchema projectionSchema)
        {
            if (_cache.TryGetDelegate(typeof(T), projectionSchema, out var func))
            {
                return (Func<ResultSet, int, T>)func;
            }

            return CreateNewDelegate<T>(projectionSchema);
        }

        private Func<ResultSet, int, T> CreateNewDelegate<T>(KuduSchema projectionSchema)
        {
            var type = typeof(T);
            var resultSet = Expression.Parameter(typeof(ResultSet), "resultSet");
            var rowIndex = Expression.Parameter(typeof(int), "rowIndex");
            var returnVal = Expression.Variable(type, "result");

            var profile = MappingProfileFactory.Create(projectionSchema, type);
            var constructor = CreateConstructorExpression(profile, resultSet, rowIndex);
            var properties = profile.Properties;

            var expressions = new List<Expression>(properties.Length + 2)
            {
                Expression.Assign(returnVal, constructor)
            };

            CreatePropertyExpressions(properties, resultSet, rowIndex, returnVal, expressions);

            expressions.Add(returnVal);

            var blockExpr = Expression.Block(type, new[] { returnVal }, expressions);

            var lambda = Expression.Lambda<Func<ResultSet, int, T>>(blockExpr, resultSet, rowIndex);
            var func = lambda.Compile();

            _cache.AddDelegate(type, projectionSchema, func);

            return func;
        }

        private static Expression CreateConstructorExpression(
            MappingProfile profile,
            ParameterExpression resultSet,
            ParameterExpression rowIndex)
        {
            var parameters = profile.ConstructorParameters;
            var arguments = new Expression[parameters.Length];

            for (int i = 0; i < arguments.Length; i++)
            {
                var parameter = parameters[i];

                var expression = GetValueExpression(
                    resultSet,
                    rowIndex,
                    parameter.ResultSetMethod,
                    parameter.ColumnIndex,
                    parameter.DestinationType);

                arguments[i] = expression;
            }

            return Expression.New(profile.Constructor, arguments);
        }

        private static void CreatePropertyExpressions(
            MappingProperty[] properties,
            ParameterExpression resultSet,
            ParameterExpression rowIndex,
            ParameterExpression returnVal,
            List<Expression> output)
        {
            foreach (var property in properties)
            {
                var propertyInfo = property.PropertyInfo;

                var getValueExpression = GetValueExpression(
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

        private static Expression GetValueExpression(
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
    }
}
