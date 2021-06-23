using System;
using System.Collections.Generic;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class KeyEncoderTests
    {
        [Fact]
        public void TestSimplePrimaryKey()
        {
            var schema = GetSchema(new TableBuilder()
                .AddColumn("key", KuduType.String, opt => opt.Key(true)));

            var row = new PartialRow(schema);
            row.SetString("key", "foo");
            Assert.Equal("foo".ToUtf8ByteArray(), KeyEncoder.EncodePrimaryKey(row));
        }

        [Fact]
        public void TestPrimaryKeyWithNull()
        {
            var schema = GetSchema(new TableBuilder()
                .AddColumn("key", KuduType.String, opt => opt.Key(true))
                .AddColumn("key2", KuduType.String, opt => opt.Key(true)));

            var row1 = new PartialRow(schema);
            row1.SetString("key", "foo");
            row1.SetString("key2", "bar");
            Assert.Equal(
                "foo\0\0bar".ToUtf8ByteArray(),
                KeyEncoder.EncodePrimaryKey(row1));

            var row2 = new PartialRow(schema);
            row2.SetString("key", "xxx\0yyy");
            row2.SetString("key2", "bar");
            Assert.Equal(
                "xxx\0\u0001yyy\0\0bar".ToUtf8ByteArray(),
                KeyEncoder.EncodePrimaryKey(row2));
        }

        [Fact]
        public void TestPrimaryKeys()
        {
            // Test that we get the correct memcmp result,
            // the bytes are in big-endian order in a key
            var schema = GetSchema(new TableBuilder()
                .AddColumn("key", KuduType.Int32, opt => opt.Key(true))
                .AddColumn("key2", KuduType.String, opt => opt.Key(true)));

            var small = new PartialRow(schema);
            small.SetInt32("key", 20);
            small.SetString("key2", "data");
            var smallPK = KeyEncoder.EncodePrimaryKey(small);
            Assert.Equal(
                new byte[] { 0x80, 0, 0, 20, (byte)'d', (byte)'a', (byte)'t', (byte)'a' },
                smallPK);

            var big = new PartialRow(schema);
            big.SetInt32("key", 10000);
            big.SetString("key2", "data");
            byte[] bigPK = KeyEncoder.EncodePrimaryKey(big);
            Assert.Equal(
                new byte[] { 0x80, 0, 0x27, 0x10, (byte)'d', (byte)'a', (byte)'t', (byte)'a' },
                bigPK);
            Assert.True(smallPK.SequenceCompareTo(bigPK) < 0);
        }

        [Fact]
        public void TestPrimaryKeyEncoding()
        {
            var schema = GetSchema(new TableBuilder()
                .AddColumn("int8", KuduType.Int8, opt => opt.Key(true))
                .AddColumn("int16", KuduType.Int16, opt => opt.Key(true))
                .AddColumn("int32", KuduType.Int32, opt => opt.Key(true))
                .AddColumn("int64", KuduType.Int64, opt => opt.Key(true))
                .AddColumn("decimal32", KuduType.Decimal32, opt => opt.Key(true)
                    .DecimalAttributes(DecimalUtil.MaxDecimal32Precision, 0))
                .AddColumn("decimal64", KuduType.Decimal64, opt => opt.Key(true)
                    .DecimalAttributes(DecimalUtil.MaxDecimal64Precision, 0))
                .AddColumn("decimal128", KuduType.Decimal128, opt => opt.Key(true)
                    .DecimalAttributes(DecimalUtil.MaxDecimal128Precision, 0))
                .AddColumn("varchar", KuduType.Varchar, opt => opt.Key(true)
                    .VarcharAttributes(10))
                .AddColumn("string", KuduType.String, opt => opt.Key(true))
                .AddColumn("binary", KuduType.Binary, opt => opt.Key(true)));

            var rowA = new PartialRow(schema);
            rowA.SetSByte("int8", sbyte.MinValue);
            rowA.SetInt16("int16", short.MinValue);
            rowA.SetInt32("int32", int.MinValue);
            rowA.SetInt64("int64", long.MinValue);
            // Note: The decimal value is not the minimum of the underlying int32,
            // int64, int128 type so we don't use "minimum" values in the test.
            rowA.SetDecimal("decimal32", 5m);
            rowA.SetDecimal("decimal64", 6m);
            rowA.SetDecimal("decimal128", 7m);
            rowA.SetString("varchar", "");
            rowA.SetString("string", "");
            rowA.SetBinary("binary", Array.Empty<byte>());

            var rowAEncoded = KeyEncoder.EncodePrimaryKey(rowA);
            Assert.Equal(new byte[]
            {
                0,
                0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0x80, 0, 0, 5,
                0x80, 0, 0, 0, 0, 0, 0, 6,
                0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
                0, 0,
                0, 0
            }, rowAEncoded);

            var rowB = new PartialRow(schema);
            rowB.SetSByte("int8", sbyte.MaxValue);
            rowB.SetInt16("int16", short.MaxValue);
            rowB.SetInt32("int32", int.MaxValue);
            rowB.SetInt64("int64", long.MaxValue);
            // Note: The decimal value is not the maximum of the underlying int32,
            // int64, int128 type so we don't use "maximum" values in the test.
            rowB.SetDecimal("decimal32", 5m);
            rowB.SetDecimal("decimal64", 6m);
            rowB.SetDecimal("decimal128", 7m);
            rowB.SetString("varchar", "abc\u0001\0defghij");
            rowB.SetString("string", "abc\u0001\0def");
            rowB.SetBinary("binary", "\0\u0001binary".ToUtf8ByteArray());

            var rowBEncoded = KeyEncoder.EncodePrimaryKey(rowB);
            Assert.Equal(ToByteArray(new int[]
            {
                0xff,
                0xff, 0xff,
                0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x80, 0, 0, 5,
                0x80, 0, 0, 0, 0, 0, 0, 6,
                0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
                'a', 'b', 'c', 1, 0, 1, 'd', 'e', 'f', 'g', 'h', 0, 0,
                'a', 'b', 'c', 1, 0, 1, 'd', 'e', 'f', 0, 0,
                0, 1, 'b', 'i', 'n', 'a', 'r', 'y'
            }), rowBEncoded);

            var rowC = new PartialRow(schema);
            rowC.SetSByte("int8", 1);
            rowC.SetInt16("int16", 2);
            rowC.SetInt32("int32", 3);
            rowC.SetInt64("int64", 4);
            rowC.SetDecimal("decimal32", 5m);
            rowC.SetDecimal("decimal64", 6m);
            rowC.SetDecimal("decimal128", 7m);
            rowC.SetString("varchar", "abc\n12345678");
            rowC.SetString("string", "abc\n123");
            rowC.SetBinary("binary", "\0\u0001\u0002\u0003\u0004\u0005".ToUtf8ByteArray());

            var rowCEncoded = KeyEncoder.EncodePrimaryKey(rowC);
            Assert.Equal(ToByteArray(new int[]
            {
                0x81,
                0x80, 2,
                0x80, 0, 0, 3,
                0x80, 0, 0, 0, 0, 0, 0, 4,
                0x80, 0, 0, 5,
                0x80, 0, 0, 0, 0, 0, 0, 6,
                0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
                'a', 'b', 'c', '\n', '1', '2', '3', '4', '5', '6', 0, 0,
                'a', 'b', 'c', '\n', '1', '2', '3', 0, 0,
                0, 1, 2, 3, 4, 5
            }), rowCEncoded);

            var rowD = new PartialRow(schema);
            rowD.SetSByte("int8", -1);
            rowD.SetInt16("int16", -2);
            rowD.SetInt32("int32", -3);
            rowD.SetInt64("int64", -4);
            rowD.SetDecimal("decimal32", -5m);
            rowD.SetDecimal("decimal64", -6m);
            rowD.SetDecimal("decimal128", -7m);
            rowD.SetString("varchar", "\0abc\n\u0001\u0001\0 123\u0001\0");
            rowD.SetString("string", "\0abc\n\u0001\u0001\0 123\u0001\0");
            rowD.SetBinary("binary", "\0\u0001\u0002\u0003\u0004\u0005\0".ToUtf8ByteArray());

            var rowDEncoded = KeyEncoder.EncodePrimaryKey(rowD);
            Assert.Equal(ToByteArray(new int[]
            {
                127,
                127, -2,
                127, -1, -1, -3,
                127, -1, -1, -1, -1, -1, -1, -4,
                127, -1, -1, -5,
                127, -1, -1, -1, -1, -1, -1, -6,
                127, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -7,
                0, 1, 'a', 'b', 'c', '\n', 1, 1, 0, 1, ' ', '1', 0, 0,
                0, 1, 'a', 'b', 'c', '\n', 1, 1, 0, 1, ' ', '1', '2', '3', 1, 0, 1, 0, 0,
                0, 1, 2, 3, 4, 5, 0,
            }), rowDEncoded);
        }

        [Fact]
        public void TestPartitionKeyEncoding()
        {
            var builder = new TableBuilder()
                .AddColumn("a", KuduType.Int32, opt => opt.Key(true))
                .AddColumn("b", KuduType.String, opt => opt.Key(true))
                .AddColumn("c", KuduType.String, opt => opt.Key(true));

            var schema = GetSchema(builder);

            var partitionSchema = new PartitionSchema(
                new RangeSchema(new List<int> { 0, 1, 2 }),
                new List<HashBucketSchema>
                {
                    new HashBucketSchema(new List<int> { 0, 1 }, 32, 0),
                    new HashBucketSchema(new List<int> { 2 }, 32, 42)
                },
                schema);

            var rowA = new PartialRow(schema);
            rowA.SetInt32(0, 0);
            rowA.SetString(1, "");
            rowA.SetString(2, "");

            CheckPartitionKey(rowA, partitionSchema, new byte[]
            {
                0, 0, 0, 0,     // hash(0, "")
                0, 0, 0, 0x14,  // hash("")
                0x80, 0, 0, 0,  // a = 0
                0, 0            // b = ""; c is elided
            });

            var rowB = new PartialRow(schema);
            rowB.SetInt32(0, 1);
            rowB.SetString(1, "");
            rowB.SetString(2, "");

            CheckPartitionKey(rowB, partitionSchema, new byte[]
            {
                0, 0, 0, 0x5,   // hash(1, "")
                0, 0, 0, 0x14,  // hash("")
                0x80, 0, 0, 1,  // a = 1
                0, 0            // b = ""; c is elided
            });

            var rowC = new PartialRow(schema);
            rowC.SetInt32(0, 0);
            rowC.SetString(1, "b");
            rowC.SetString(2, "c");

            CheckPartitionKey(rowC, partitionSchema, new byte[]
            {
                0, 0, 0, 0x1A,      // hash(0, "b")
                0, 0, 0, 0x1D,      // hash("c")
                0x80, 0, 0, 0,      // a = 0
                (byte)'b', 0, 0,    // b = "b"
                (byte)'c'           // c = "c"
            });

            var rowD = new PartialRow(schema);
            rowD.SetInt32(0, 1);
            rowD.SetString(1, "b");
            rowD.SetString(2, "c");

            CheckPartitionKey(rowD, partitionSchema, new byte[]
            {
                0, 0, 0, 0,         // hash(1, "b")
                0, 0, 0, 0x1D,      // hash("c")
                0x80, 0, 0, 1,      // a = 1
                (byte)'b', 0, 0,    // b = "b"
                (byte)'c'           // c = "c"
            });
        }

        private static void CheckPartitionKey(
            PartialRow row,
            PartitionSchema partitionSchema,
            byte[] expectedPartitionKey)
        {
            var maxSize = KeyEncoder.CalculateMaxPartitionKeySize(row, partitionSchema);
            Span<byte> buffer = stackalloc byte[maxSize];

            KeyEncoder.EncodePartitionKey(
                row,
                partitionSchema,
                buffer,
                out int bytesWritten);

            var partitionKey = buffer.Slice(0, bytesWritten).ToArray();
            Assert.Equal(expectedPartitionKey, partitionKey);
        }

        private static KuduSchema GetSchema(TableBuilder builder)
        {
            var request = builder.Build();
            // The builder doesn't support setting column ids, so for testing
            // set them here, and use the column index as the id.
            for (var i = 0; i < request.Schema.Columns.Count; i++)
            {
                request.Schema.Columns[i].Id = (uint)i;
            }
            return new KuduSchema(request.Schema);
        }

        private static byte[] ToByteArray(int[] source)
        {
            var bytes = new byte[source.Length];

            for (int i = 0; i < bytes.Length; i++)
                bytes[i] = (byte)source[i];

            return bytes;
        }
    }
}
