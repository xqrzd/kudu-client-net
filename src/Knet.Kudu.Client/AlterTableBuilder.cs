using System;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Master;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    public class AlterTableBuilder
    {
        private readonly AlterTableRequestPB _request;

        public AlterTableBuilder()
        {
            _request = new AlterTableRequestPB();
        }

        /// <summary>
        /// Change a table's name.
        /// </summary>
        /// <param name="newName">New table's name, must be used to check progress.</param>
        public AlterTableBuilder RenameTable(string newName)
        {
            _request.NewTableName = newName;
            return this;
        }

        /// <summary>
        /// Add a new column.
        /// </summary>
        /// <param name="columnBuilder">The column to add.</param>
        public AlterTableBuilder AddColumn(ColumnBuilder columnBuilder)
        {
            ColumnSchemaPB schema = columnBuilder;

            if (!schema.IsNullable && schema.ReadDefaultValue == null)
                throw new ArgumentException("A new non-null column must have a default value");

            if (schema.IsKey)
                throw new ArgumentException("Key columns cannot be added");

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AddColumn,
                AddColumn = new AlterTableRequestPB.AddColumn
                {
                    Schema = schema
                }
            });

            return this;
        }

        /// <summary>
        /// Drop a column.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        public AlterTableBuilder DropColumn(string name)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.DropColumn,
                DropColumn = new AlterTableRequestPB.DropColumn
                {
                    Name = name
                }
            });

            return this;
        }

        /// <summary>
        /// Change the name of a column.
        /// </summary>
        /// <param name="oldName">Old column's name, must exist.</param>
        /// <param name="newName">New name to use.</param>
        public AlterTableBuilder RenameColumn(string oldName, string newName)
        {
            // For backwards compatibility, this uses the RENAME_COLUMN step type.
            // Needed for Kudu 1.3, the oldest supported version.
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.RenameColumn,
                RenameColumn = new AlterTableRequestPB.RenameColumn
                {
                    OldName = oldName,
                    NewName = newName
                }
            });

            return this;
        }

        /// <summary>
        /// Remove the default value for a column.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        public AlterTableBuilder RemoveDefault(string name)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        RemoveDefault = true
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the default value for a column. `newDefault` must not be null or
        /// else throws.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="newDefault">The new default value.</param>
        public AlterTableBuilder ChangeDefault(string name, object newDefault)
        {
            if (newDefault == null)
            {
                throw new ArgumentException("newDefault cannot be null: " +
                    "use RemoveDefault to clear a default value");
            }

            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        DefaultValue = KuduEncoder.EncodeDefaultValue(newDefault)
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the block size of a column's storage. A nonpositive value
        /// indicates a server-side default.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="blockSize">The new block size.</param>
        public AlterTableBuilder ChangeDesiredBlockSize(string name, int blockSize)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        BlockSize = blockSize
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the encoding used for a column.
        /// </summary>
        /// <param name="name">Name of the column.</param>
        /// <param name="encoding">The new encoding.</param>
        public AlterTableBuilder ChangeEncoding(string name, EncodingType encoding)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        Encoding = (EncodingTypePB)encoding
                    }
                }
            });

            return this;
        }

        /// <summary>
        /// Change the compression used for a column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="compressionType">The new compression algorithm.</param>
        public AlterTableBuilder ChangeCompressionAlgorithm(
            string name, CompressionType compressionType)
        {
            _request.AlterSchemaSteps.Add(new AlterTableRequestPB.Step
            {
                Type = AlterTableRequestPB.StepType.AlterColumn,
                AlterColumn = new AlterTableRequestPB.AlterColumn
                {
                    Delta = new ColumnSchemaDeltaPB
                    {
                        Name = name,
                        Compression = (CompressionTypePB)compressionType
                    }
                }
            });

            return this;
        }

        public static implicit operator AlterTableRequestPB(AlterTableBuilder builder) => builder._request;
    }
}
