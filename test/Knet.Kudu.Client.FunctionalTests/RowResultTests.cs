using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using Knet.Kudu.Client.Internal;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests;

[MiniKuduClusterTest]
public class RowResultTests
{
    [SkippableTheory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TestNonNullRows(bool includeNullsInSchema)
    {
        await using var miniCluster = await new MiniKuduClusterBuilder().BuildAsync();
        await using var client = miniCluster.CreateClient();
        await using var session = client.NewSession();

        var builder = ClientTestUtil.CreateAllTypesSchema(includeNullsInSchema)
            .SetTableName(nameof(TestNonNullRows))
            .CreateBasicRangePartition();

        var table = await client.CreateTableAsync(builder);

        int numRows = 5;
        int currentRow = 0;
        for (int i = 0; i < numRows; i++)
        {
            var insert = ClientTestUtil.CreateAllTypesInsert(table, i);
            await session.EnqueueAsync(insert);
        }

        await session.FlushAsync();

        var scanner = client.NewScanBuilder(table).Build();

        await foreach (var resultSet in scanner)
        {
            foreach (var row in resultSet)
            {
                Assert.Equal(currentRow, row.GetInt32("key"));
                Assert.Equal(currentRow, row.GetNullableInt32("key"));
                Assert.Equal(KuduEncoder.EncodeInt32(currentRow), row.GetSpan("key").ToArray());
                Assert.Equal(42, row.GetByte("int8"));
                Assert.Equal((byte?)42, row.GetNullableByte("int8"));
                Assert.Equal(42, row.GetSByte("int8"));
                Assert.Equal((sbyte?)42, row.GetNullableSByte("int8"));
                Assert.Equal(43, row.GetInt16("int16"));
                Assert.Equal((short?)43, row.GetNullableInt16("int16"));
                Assert.Equal(44, row.GetInt32("int32"));
                Assert.Equal(44, row.GetNullableInt32("int32"));
                Assert.Equal(45, row.GetInt64("int64"));
                Assert.Equal(45, row.GetNullableInt64("int64"));
                Assert.True(row.GetBool("bool"));
                Assert.True(row.GetNullableBool("bool"));
                Assert.Equal(52.35f, row.GetFloat("float"));
                Assert.Equal(52.35f, row.GetNullableFloat("float"));
                Assert.Equal(53.35, row.GetDouble("double"));
                Assert.Equal(53.35, row.GetNullableDouble("double"));
                Assert.Equal("fun with ütf\0", row.GetString("string"));
                Assert.Equal("fun with ütf\0", row.GetNullableString("string"));
                Assert.Equal("árvíztűrő ", row.GetString("varchar"));
                Assert.Equal("árvíztűrő ", row.GetNullableString("varchar"));
                Assert.Equal(new byte[] { 0, 1, 2, 3, 4 }, row.GetBinary("binary"));
                Assert.Equal(new byte[] { 0, 1, 2, 3, 4 }, row.GetSpan("binary").ToArray());
                Assert.Equal(DateTime.Parse("8/19/2020 7:50 PM").ToUniversalTime(), row.GetDateTime("timestamp"));
                Assert.Equal(DateTime.Parse("8/19/2020 7:50 PM").ToUniversalTime(), row.GetNullableDateTime("timestamp"));
                Assert.Equal(DateTime.Parse("8/19/2020").ToUniversalTime().Date, row.GetDateTime("date"));
                Assert.Equal(DateTime.Parse("8/19/2020").ToUniversalTime().Date, row.GetNullableDateTime("date"));
                Assert.Equal(12.345m, row.GetDecimal("decimal32"));
                Assert.Equal(12.345m, row.GetNullableDecimal("decimal32"));
                Assert.Equal(12.346m, row.GetDecimal("decimal64"));
                Assert.Equal(12.346m, row.GetNullableDecimal("decimal64"));
                Assert.Equal(12.347m, row.GetDecimal("decimal128"));
                Assert.Equal(12.347m, row.GetNullableDecimal("decimal128"));
                currentRow++;
            }
        }

        Assert.Equal(numRows, currentRow);
    }

    [SkippableFact]
    public async Task TestNullRows()
    {
        await using var miniCluster = await new MiniKuduClusterBuilder().BuildAsync();
        await using var client = miniCluster.CreateClient();
        await using var session = client.NewSession();

        var builder = ClientTestUtil.CreateAllTypesSchema(true)
            .SetTableName(nameof(TestNullRows))
            .CreateBasicRangePartition();

        var table = await client.CreateTableAsync(builder);

        int numRows = 5;
        int currentRow = 0;
        for (int i = 0; i < numRows; i++)
        {
            var insert = ClientTestUtil.CreateAllNullsInsert(table, i);
            await session.EnqueueAsync(insert);
        }

        await session.FlushAsync();

        var scanner = client.NewScanBuilder(table).Build();

        await foreach (var resultSet in scanner)
        {
            foreach (var row in resultSet)
            {
                Assert.Equal(currentRow, row.GetInt32("key"));
                Assert.Equal(currentRow, row.GetNullableInt32("key"));

                Assert.True(row.IsNull("int8"));
                Assert.True(row.IsNull("int16"));
                Assert.True(row.IsNull("int32"));
                Assert.True(row.IsNull("int64"));
                Assert.True(row.IsNull("bool"));
                Assert.True(row.IsNull("float"));
                Assert.True(row.IsNull("double"));
                Assert.True(row.IsNull("string"));
                Assert.True(row.IsNull("varchar"));
                Assert.True(row.IsNull("binary"));
                Assert.True(row.IsNull("timestamp"));
                Assert.True(row.IsNull("date"));
                Assert.True(row.IsNull("decimal32"));
                Assert.True(row.IsNull("decimal64"));
                Assert.True(row.IsNull("decimal128"));

                Assert.Null(row.GetNullableByte("int8"));
                Assert.Null(row.GetNullableSByte("int8"));
                Assert.Null(row.GetNullableInt16("int16"));
                Assert.Null(row.GetNullableInt32("int32"));
                Assert.Null(row.GetNullableInt64("int64"));
                Assert.Null(row.GetNullableBool("bool"));
                Assert.Null(row.GetNullableFloat("float"));
                Assert.Null(row.GetNullableDouble("double"));
                Assert.Null(row.GetNullableString("string"));
                Assert.Null(row.GetNullableString("varchar"));
                Assert.Null(row.GetNullableBinary("binary"));
                Assert.Null(row.GetNullableDateTime("timestamp"));
                Assert.Null(row.GetNullableDateTime("date"));
                Assert.Null(row.GetNullableDecimal("decimal32"));
                Assert.Null(row.GetNullableDecimal("decimal64"));
                Assert.Null(row.GetNullableDecimal("decimal128"));

                Assert.Equal(0, row.GetSpan("int8").Length);
                Assert.Equal(0, row.GetSpan("int16").Length);
                Assert.Equal(0, row.GetSpan("int32").Length);
                Assert.Equal(0, row.GetSpan("int64").Length);
                Assert.Equal(0, row.GetSpan("bool").Length);
                Assert.Equal(0, row.GetSpan("float").Length);
                Assert.Equal(0, row.GetSpan("double").Length);
                Assert.Equal(0, row.GetSpan("string").Length);
                Assert.Equal(0, row.GetSpan("varchar").Length);
                Assert.Equal(0, row.GetSpan("binary").Length);
                Assert.Equal(0, row.GetSpan("timestamp").Length);
                Assert.Equal(0, row.GetSpan("date").Length);
                Assert.Equal(0, row.GetSpan("decimal32").Length);
                Assert.Equal(0, row.GetSpan("decimal64").Length);
                Assert.Equal(0, row.GetSpan("decimal128").Length);
                currentRow++;
            }
        }

        Assert.Equal(numRows, currentRow);
    }

    [SkippableFact]
    public async Task TestEmptyProjection()
    {
        await using var miniCluster = await new MiniKuduClusterBuilder().BuildAsync();
        await using var client = miniCluster.CreateClient();
        await using var session = client.NewSession();

        var builder = ClientTestUtil.CreateAllTypesSchema()
            .SetTableName(nameof(TestEmptyProjection))
            .CreateBasicRangePartition();

        var table = await client.CreateTableAsync(builder);

        int numRows = 5;
        for (int i = 0; i < numRows; i++)
        {
            var insert = ClientTestUtil.CreateAllTypesInsert(table, i);
            await session.EnqueueAsync(insert);
        }

        await session.FlushAsync();

        var scanner = client.NewScanBuilder(table)
            .SetEmptyProjection()
            .Build();

        long scannedRows = 0;
        await foreach (var resultSet in scanner)
        {
            scannedRows += resultSet.Count;
            Assert.Empty(resultSet.Schema.Columns);
            Assert.Empty(resultSet);
        }

        Assert.Equal(numRows, scannedRows);
    }

    [SkippableFact]
    public async Task TestResultSetDispose()
    {
        await using var miniCluster = await new MiniKuduClusterBuilder().BuildAsync();
        await using var client = miniCluster.CreateClient();
        await using var session = client.NewSession();

        var builder = ClientTestUtil.CreateAllTypesSchema()
            .SetTableName(nameof(TestResultSetDispose))
            .CreateBasicRangePartition();

        var table = await client.CreateTableAsync(builder);

        int numRows = 5;
        for (int i = 0; i < numRows; i++)
        {
            var insert = ClientTestUtil.CreateAllTypesInsert(table, i);
            await session.EnqueueAsync(insert);
        }

        await session.FlushAsync();

        var scanner = client.NewScanBuilder(table)
            .Build();

        long scannedRows = 0;
        await foreach (var resultSet in scanner)
        {
            foreach (var row in resultSet)
            {
                scannedRows++;
                resultSet.Invalidate();

                Assert.Throws<ObjectDisposedException>(() => row.GetInt32("key"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableInt32("key"));
                Assert.Throws<ObjectDisposedException>(() => row.GetSpan("key"));
                Assert.Throws<ObjectDisposedException>(() => row.GetByte("int8"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableByte("int8"));
                Assert.Throws<ObjectDisposedException>(() => row.GetSByte("int8"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableSByte("int8"));
                Assert.Throws<ObjectDisposedException>(() => row.GetInt16("int16"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableInt16("int16"));
                Assert.Throws<ObjectDisposedException>(() => row.GetInt32("int32"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableInt32("int32"));
                Assert.Throws<ObjectDisposedException>(() => row.GetInt64("int64"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableInt64("int64"));
                Assert.Throws<ObjectDisposedException>(() => row.GetBool("bool"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableBool("bool"));
                Assert.Throws<ObjectDisposedException>(() => row.GetFloat("float"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableFloat("float"));
                Assert.Throws<ObjectDisposedException>(() => row.GetDouble("double"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableDouble("double"));
                Assert.Throws<ObjectDisposedException>(() => row.GetString("string"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableString("string"));
                Assert.Throws<ObjectDisposedException>(() => row.GetString("varchar"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableString("varchar"));
                Assert.Throws<ObjectDisposedException>(() => row.GetBinary("binary"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableBinary("binary"));
                Assert.Throws<ObjectDisposedException>(() => row.GetDateTime("timestamp"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableDateTime("timestamp"));
                Assert.Throws<ObjectDisposedException>(() => row.GetDateTime("date"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableDateTime("date"));
                Assert.Throws<ObjectDisposedException>(() => row.GetDecimal("decimal32"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableDecimal("decimal32"));
                Assert.Throws<ObjectDisposedException>(() => row.GetDecimal("decimal64"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableDecimal("decimal64"));
                Assert.Throws<ObjectDisposedException>(() => row.GetDecimal("decimal128"));
                Assert.Throws<ObjectDisposedException>(() => row.GetNullableDecimal("decimal128"));
                Assert.Throws<ObjectDisposedException>(() => row.HasIsDeleted);
                Assert.Throws<ObjectDisposedException>(() => row.IsDeleted);
            }
        }

        Assert.Equal(numRows, scannedRows);
    }
}
