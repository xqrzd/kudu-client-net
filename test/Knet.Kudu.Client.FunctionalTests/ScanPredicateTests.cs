using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using Knet.Kudu.Client.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class ScanPredicateTests : IAsyncLifetime
    {
        private KuduTestHarness _harness;
        private KuduClient _client;
        private IKuduSession _session;

        public async Task InitializeAsync()
        {
            _harness = await new MiniKuduClusterBuilder().BuildHarnessAsync();
            _client = _harness.CreateClient();
            _session = _client.NewSession();
        }

        public async Task DisposeAsync()
        {
            await _session.DisposeAsync();
            await _client.DisposeAsync();
            await _harness.DisposeAsync();
        }

        [SkippableFact]
        public async Task TestBoolPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("bool-table")
                .AddColumn("value", KuduType.Bool);

            var table = await _client.CreateTableAsync(builder);

            var values = new SortedSet<bool> { true, false };
            var testValues = new List<bool> { true, false };

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetBool("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestBytePredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("byte-table")
                .AddColumn("value", KuduType.Int8);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateIntegerValues(KuduType.Int8);

            long i = 0;
            foreach (byte value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetByte("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, CreateIntegerTestValues(KuduType.Int8));
        }

        [SkippableFact]
        public async Task TestShortPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("short-table")
                .AddColumn("value", KuduType.Int16);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateIntegerValues(KuduType.Int16);

            long i = 0;
            foreach (short value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetInt16("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, CreateIntegerTestValues(KuduType.Int16));
        }

        [SkippableFact]
        public async Task TestIntPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("int-table")
                .AddColumn("value", KuduType.Int32);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateIntegerValues(KuduType.Int32);

            long i = 0;
            foreach (int value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetInt32("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, CreateIntegerTestValues(KuduType.Int32));
        }

        [SkippableFact]
        public async Task TestLongPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("long-table")
                .AddColumn("value", KuduType.Int64);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateIntegerValues(KuduType.Int64);

            long i = 0;
            foreach (long value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetInt64("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, CreateIntegerTestValues(KuduType.Int64));
        }

        [SkippableFact]
        public async Task TestTimestampPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("timestamp-table")
                .AddColumn("value", KuduType.UnixtimeMicros);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateTimestampValues();
            var testValues = CreateTimestampTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetDateTime("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestDatePredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("date-table")
                .AddColumn("value", KuduType.Date);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateDateValues();
            var testValues = CreateDateTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetDateTime("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestFloatPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("float-table")
                .AddColumn("value", KuduType.Float);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateFloatValues();
            var testValues = CreateFloatTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetFloat("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestDoublePredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("double-table")
                .AddColumn("value", KuduType.Double);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateDoubleValues();
            var testValues = CreateDoubleTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetDouble("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestDecimalPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("decimal-table")
                .AddColumn("value", KuduType.Decimal32, opt => opt
                    .DecimalAttributes(4, 2));

            var table = await _client.CreateTableAsync(builder);

            var values = CreateDecimalValues();
            var testValues = CreateDecimalTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetDecimal("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestStringPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("string-table")
                .AddColumn("value", KuduType.String);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateStringValues();
            var testValues = CreateStringTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetString("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestVarcharPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("varchar-table")
                .AddColumn("value", KuduType.Varchar, opt => opt.VarcharAttributes(10));

            var table = await _client.CreateTableAsync(builder);

            var values = CreateStringValues();
            var testValues = CreateStringTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetString("value", value);
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            await CheckPredicatesAsync(table, values, testValues);
        }

        [SkippableFact]
        public async Task TestBinaryPredicates()
        {
            var builder = GetDefaultTableBuilder()
                .SetTableName("binary-table")
                .AddColumn("value", KuduType.Binary);

            var table = await _client.CreateTableAsync(builder);

            var values = CreateStringValues();
            var testValues = CreateStringTestValues();

            long i = 0;
            foreach (var value in values)
            {
                var insert = table.NewInsert();
                insert.SetInt64("key", i++);
                insert.SetBinary("value", value.ToUtf8ByteArray());
                await _session.EnqueueAsync(insert);
            }

            var nullInsert = table.NewInsert();
            nullInsert.SetInt64("key", i);
            nullInsert.SetNull("value");
            await _session.EnqueueAsync(nullInsert);
            await _session.FlushAsync();

            var col = table.Schema.GetColumn("value");
            Assert.Equal(values.Count + 1, await CountRowsAsync(table));

            foreach (var s in testValues)
            {
                var v = s.ToUtf8ByteArray();

                // value = v
                var equal = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.Equal, v);
                Assert.Equal(values.GetViewBetween(s, s).Count, await CountRowsAsync(table, equal));

                // value >= v
                var greaterEqual = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.GreaterEqual, v);
                Assert.Equal(values.TailSet(s).Count, await CountRowsAsync(table, greaterEqual));

                // value <= v
                var lessEqual = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.LessEqual, v);
                Assert.Equal(values.HeadSet(s, true).Count, await CountRowsAsync(table, lessEqual));

                // value > v
                var greater = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.Greater, v);
                Assert.Equal(values.TailSet(s, false).Count, await CountRowsAsync(table, greater));

                // value < v
                var less = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.Less, v);
                Assert.Equal(values.HeadSet(s).Count, await CountRowsAsync(table, less));
            }

            var isNotNull = KuduPredicate.NewIsNotNullPredicate(col);
            Assert.Equal(values.Count, await CountRowsAsync(table, isNotNull));

            var isNull = KuduPredicate.NewIsNullPredicate(col);
            Assert.Equal(1, await CountRowsAsync(table, isNull));
        }

        private TableBuilder GetDefaultTableBuilder()
        {
            return new TableBuilder()
                .AddColumn("key", KuduType.Int64, opt => opt.Key(true))
                .SetRangePartitionColumns("key");
        }

        private async Task<int> CountRowsAsync(KuduTable table, params KuduPredicate[] predicates)
        {
            var scanBuilder = _client.NewScanBuilder(table);

            foreach (var predicate in predicates)
                scanBuilder.AddPredicate(predicate);

            var scanner = scanBuilder.Build();
            int count = 0;

            await foreach (var resultSet in scanner)
            {
                count += resultSet.Count;
            }

            return count;
        }

        private SortedSet<long> CreateIntegerValues(KuduType type)
        {
            var values = new SortedSet<long>();
            for (long i = -50; i < 50; i++)
            {
                values.Add(i);
            }
            values.Add(KuduPredicate.MinIntValue(type));
            values.Add(KuduPredicate.MinIntValue(type) + 1);
            values.Add(KuduPredicate.MaxIntValue(type) - 1);
            values.Add(KuduPredicate.MaxIntValue(type));
            return values;
        }

        private List<long> CreateIntegerTestValues(KuduType type)
        {
            return new List<long>
            {
                KuduPredicate.MinIntValue(type),
                KuduPredicate.MinIntValue(type) + 1,
                -51L,
                -50L,
                0L,
                49L,
                50L,
                KuduPredicate.MaxIntValue(type) - 1,
                KuduPredicate.MaxIntValue(type)
            };
        }

        private SortedSet<DateTime> CreateTimestampValues()
        {
            var epoch = EpochTime.UnixEpoch;
            var values = new SortedSet<DateTime>();
            for (long i = -500; i < 500; i += 10)
            {
                values.Add(epoch.AddTicks(i));
            }
            values.Add(DateTimeOffset.MinValue.UtcDateTime);
            values.Add(DateTimeOffset.MinValue.UtcDateTime.AddTicks(10));
            values.Add(DateTimeOffset.MaxValue.UtcDateTime.AddTicks(-10));
            values.Add(DateTimeOffset.MaxValue.UtcDateTime);
            return values;
        }

        private List<DateTime> CreateTimestampTestValues()
        {
            return new List<DateTime>
            {
                DateTimeOffset.MinValue.UtcDateTime,
                DateTimeOffset.MinValue.UtcDateTime.AddTicks(10),
                EpochTime.UnixEpoch.AddTicks(-510),
                EpochTime.UnixEpoch.AddTicks(-500),
                EpochTime.UnixEpoch,
                EpochTime.UnixEpoch.AddTicks(490),
                EpochTime.UnixEpoch.AddTicks(500),
                DateTimeOffset.MaxValue.UtcDateTime.AddTicks(-10),
                DateTimeOffset.MaxValue.UtcDateTime
            };
        }

        private SortedSet<DateTime> CreateDateValues()
        {
            var values = new SortedSet<DateTime>();
            for (long i = -50; i < 50; i++)
            {
                values.Add(EpochTime.UnixEpoch.AddDays(i));
            }
            values.Add(EpochTime.FromUnixTimeDays((int)KuduPredicate.MinIntValue(KuduType.Date)));
            values.Add(EpochTime.FromUnixTimeDays((int)KuduPredicate.MinIntValue(KuduType.Date) + 1));
            values.Add(EpochTime.FromUnixTimeDays((int)KuduPredicate.MaxIntValue(KuduType.Date) - 1));
            values.Add(EpochTime.FromUnixTimeDays((int)KuduPredicate.MaxIntValue(KuduType.Date)));
            return values;
        }

        private List<DateTime> CreateDateTestValues()
        {
            return new List<DateTime>
            {
                EpochTime.FromUnixTimeDays((int)KuduPredicate.MinIntValue(KuduType.Date)),
                EpochTime.FromUnixTimeDays((int)KuduPredicate.MinIntValue(KuduType.Date) + 1),
                EpochTime.UnixEpoch.AddDays(-51),
                EpochTime.UnixEpoch.AddDays(-50),
                EpochTime.UnixEpoch,
                EpochTime.UnixEpoch.AddDays(49),
                EpochTime.UnixEpoch.AddDays(50),
                EpochTime.FromUnixTimeDays((int)KuduPredicate.MaxIntValue(KuduType.Date) - 1),
                EpochTime.FromUnixTimeDays((int)KuduPredicate.MaxIntValue(KuduType.Date))
            };
        }

        private SortedSet<float> CreateFloatValues()
        {
            var values = new SortedSet<float>();
            for (long i = -50; i < 50; i++)
            {
                values.Add(i + i / 100.0f);
            }

            values.Add(float.NegativeInfinity);
            values.Add(-float.MaxValue);
            values.Add(-float.Epsilon);
            values.Add(-float.MinValue);
            values.Add(float.MinValue);
            values.Add(float.Epsilon);
            values.Add(float.MaxValue);
            values.Add(float.PositiveInfinity);

            return values;
        }

        private List<float> CreateFloatTestValues()
        {
            return new List<float>
            {
                float.NegativeInfinity,
                -float.MaxValue,
                -100.0F,
                -1.1F,
                -1.0F,
                -float.Epsilon,
                -float.MinValue,
                0.0F,
                float.MinValue,
                float.Epsilon,
                1.0F,
                1.1F,
                100.0F,
                float.MaxValue,
                float.PositiveInfinity
            };
        }

        private SortedSet<double> CreateDoubleValues()
        {
            var values = new SortedSet<double>();
            for (long i = -50; i < 50; i++)
            {
                values.Add(i + i / 100.0);
            }

            values.Add(double.NegativeInfinity);
            values.Add(-double.MaxValue);
            values.Add(-double.Epsilon);
            values.Add(-double.MinValue);
            values.Add(double.MinValue);
            values.Add(double.Epsilon);
            values.Add(double.MaxValue);
            values.Add(double.PositiveInfinity);

            return values;
        }

        private List<double> CreateDoubleTestValues()
        {
            return new List<double>
            {
                double.NegativeInfinity,
                -double.MaxValue,
                -100.0,
                -1.1,
                -1.0,
                -double.Epsilon,
                -double.MinValue,
                0.0,
                double.MinValue,
                double.Epsilon,
                1.0,
                1.1,
                100.0,
                double.MaxValue,
                double.PositiveInfinity
            };
        }

        // Returns a vector of decimal(4, 2) numbers from -50.50 (inclusive) to 50.50
        // (exclusive) (100 values) and boundary values.
        private SortedSet<decimal> CreateDecimalValues()
        {
            var values = new SortedSet<decimal>();
            for (long i = -50; i < 50; i++)
            {
                var value = i + (i * .01m);
                values.Add(value);
            }

            values.Add(-99.99m);
            values.Add(-99.98m);
            values.Add(99.98m);
            values.Add(99.99m);

            return values;
        }

        private List<decimal> CreateDecimalTestValues()
        {
            return new List<decimal>
            {
                -99.99m,
                -99.98m,
                -51.00m,
                -50.00m,
                0.00m,
                49.00m,
                50.00m,
                99.98m,
                99.99m
            };
        }

        private SortedSet<string> CreateStringValues()
        {
            return new SortedSet<string>(StringComparer.Ordinal)
            {
                "",
                "\0",
                "\0\0",
                "a",
                "a\0",
                "a\0a",
                "aa\0"
            };
        }

        private List<string> CreateStringTestValues()
        {
            return new List<string>(CreateStringValues())
            {
                "aa",
                "\u0001",
                "a\u0001"
            };
        }

        private async Task CheckPredicatesAsync<T>(
            KuduTable table,
            SortedSet<T> values,
            List<T> testValues)
        {
            var col = table.Schema.GetColumn("value");
            Assert.Equal(values.Count + 1, await CountRowsAsync(table));

            foreach (var v in testValues)
            {
                // value = v
                var equal = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.Equal, (dynamic)v);
                Assert.Equal(values.Contains(v) ? 1 : 0, await CountRowsAsync(table, equal));

                // value >= v
                var greaterEqual = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.GreaterEqual, (dynamic)v);
                Assert.Equal(values.TailSet(v).Count, await CountRowsAsync(table, greaterEqual));

                // value <= v
                var lessEqual = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.LessEqual, (dynamic)v);
                Assert.Equal(values.HeadSet(v, true).Count, await CountRowsAsync(table, lessEqual));

                // value > v
                var greater = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.Greater, (dynamic)v);
                Assert.Equal(values.TailSet(v, false).Count, await CountRowsAsync(table, greater));

                // value < v
                var less = KuduPredicate.NewComparisonPredicate(col, ComparisonOp.Less, (dynamic)v);
                Assert.Equal(values.HeadSet(v).Count, await CountRowsAsync(table, less));
            }

            var isNotNull = KuduPredicate.NewIsNotNullPredicate(col);
            Assert.Equal(values.Count, await CountRowsAsync(table, isNotNull));

            var isNull = KuduPredicate.NewIsNullPredicate(col);
            Assert.Equal(1, await CountRowsAsync(table, isNull));
        }
    }
}
