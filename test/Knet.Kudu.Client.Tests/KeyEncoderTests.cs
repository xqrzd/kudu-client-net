using System;
using System.Collections.Generic;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Tablet;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class KeyEncoderTests
    {
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
            CreateTableRequestPB request = builder;
            // The builder doesn't support setting column ids, so for testing
            // set them here, and use the column index as the id.
            for (var i = 0; i < request.Schema.Columns.Count; i++)
            {
                request.Schema.Columns[i].Id = (uint)i;
            }
            return new KuduSchema(request.Schema);
        }
    }
}
