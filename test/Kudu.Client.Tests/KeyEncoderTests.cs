using System.Collections.Generic;
using Kudu.Client.Builder;
using Kudu.Client.Internal;
using Kudu.Client.Protocol.Master;
using Kudu.Client.Tablet;
using Xunit;

namespace Kudu.Client.Tests
{
    public class KeyEncoderTests
    {
        [Fact]
        public void TestPartitionKeyEncoding()
        {
            var builder = new TableBuilder()
                .AddColumn(c =>
                {
                    c.Name = "a";
                    c.Type = KuduType.Int32;
                    c.IsKey = true;
                })
                .AddColumn(c =>
                {
                    c.Name = "b";
                    c.Type = KuduType.String;
                    c.IsKey = true;
                })
                .AddColumn(c =>
                {
                    c.Name = "c";
                    c.Type = KuduType.String;
                    c.IsKey = true;
                });

            var partitionSchema = new PartitionSchema(
                new RangeSchema(new List<int> { 0, 1, 2 }),
                new List<HashBucketSchema>
                {
                    new HashBucketSchema(new List<int> { 0, 1 }, 32, 0),
                    new HashBucketSchema(new List<int> { 2 }, 32, 42)
                });

            var schema = GetSchema(builder);

            var rowA = new PartialRow(schema, RowOperation.Insert);
            rowA.SetInt32(0, 0);
            rowA.SetString(1, "");
            rowA.SetString(2, "");

            using (var writer = new BufferWriter(32))
            {
                KeyEncoder.EncodePartitionKey(rowA, partitionSchema, writer);

                var expected = new byte[]
                {
                    0, 0, 0, 0,     // hash(0, "")
                    0, 0, 0, 0x14,  // hash("")
                    0x80, 0, 0, 0,  // a = 0
                    0, 0            // b = ""; c is elided
                };

                Assert.Equal(expected, writer.Memory.ToArray());
            }

            var rowB = new PartialRow(schema);
            rowB.SetInt32(0, 1);
            rowB.SetString(1, "");
            rowB.SetString(2, "");

            using (var writer = new BufferWriter(32))
            {
                KeyEncoder.EncodePartitionKey(rowB, partitionSchema, writer);

                var expected = new byte[]
                {
                    0, 0, 0, 0x5,   // hash(1, "")
                    0, 0, 0, 0x14,  // hash("")
                    0x80, 0, 0, 1,  // a = 1
                    0, 0            // b = ""; c is elided
                };

                Assert.Equal(expected, writer.Memory.ToArray());
            }

            var rowC = new PartialRow(schema);
            rowC.SetInt32(0, 0);
            rowC.SetString(1, "b");
            rowC.SetString(2, "c");

            using (var writer = new BufferWriter(32))
            {
                KeyEncoder.EncodePartitionKey(rowC, partitionSchema, writer);

                var expected = new byte[]
                {
                    0, 0, 0, 0x1A,      // hash(0, "b")
                    0, 0, 0, 0x1D,      // hash("c")
                    0x80, 0, 0, 0,      // a = 0
                    (byte)'b', 0, 0,    // b = "b"
                    (byte)'c'           // c = "c"
                };

                Assert.Equal(expected, writer.Memory.ToArray());
            }

            var rowD = new PartialRow(schema);
            rowD.SetInt32(0, 1);
            rowD.SetString(1, "b");
            rowD.SetString(2, "c");

            using (var writer = new BufferWriter(32))
            {
                KeyEncoder.EncodePartitionKey(rowD, partitionSchema, writer);

                var expected = new byte[]
                {
                    0, 0, 0, 0,         // hash(1, "b")
                    0, 0, 0, 0x1D,      // hash("c")
                    0x80, 0, 0, 1,      // a = 1
                    (byte)'b', 0, 0,    // b = "b"
                    (byte)'c'           // c = "c"
                };

                Assert.Equal(expected, writer.Memory.ToArray());
            }
        }

        private static Schema GetSchema(TableBuilder builder)
        {
            CreateTableRequestPB request = builder;
            // The builder doesn't support setting column ids, so for testing
            // set them here, and use the column index as the id.
            for (var i = 0; i < request.Schema.Columns.Count; i++)
            {
                request.Schema.Columns[i].Id = (uint)i;
            }
            return new Schema(request.Schema);
        }
    }
}
