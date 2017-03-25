using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{

    public interface ICQLIndex : IDDLStmt, IEquatable<string>
    {
        bool IsCustom { get; }
        ICQLTable Table { get; }
        IEnumerable<CQLFunctionColumn> Columns { get; }
        string UsingClass { get; }
        Dictionary<string,object> WithOptions { get; }
    }

    public class CQLIndex : ICQLIndex, IEquatable<ICQLIndex>, IEquatable<ICQLTable>
    {
        public CQLIndex(IFilePath cqlFile,
                        uint lineNbr,
                        string name,
                        ICQLTable table,
                        IEnumerable<CQLFunctionColumn> columns,
                        bool isCustom,
                        string usingClass,
                        Dictionary<string, object> withOptions,
                        string ddl,
                        INode defindingNode = null,
                        bool associateIndexToKeyspace = true)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException("CQLIndex name cannot be null");
            if (table == null) throw new NullReferenceException("CQLIndex must be associated to a CQL Table. It cannot be null");
            if (columns == null || columns.IsEmpty()) throw new NullReferenceException("CQLIndex must have columns (cannot be null or a count of zero)");
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException("CQLIndex must have a DDL string");

            this.Path = cqlFile;
            this.Table = table;
            this.Node = defindingNode ?? table.Node;
            this.LineNbr = lineNbr;
            this.Name = StringHelpers.RemoveQuotes(name.Trim());
            this.DDL = ddl;
            this.Columns = columns;
            this.UsingClass = string.IsNullOrEmpty(usingClass) ? null : StringHelpers.RemoveQuotes(usingClass.Trim());
            this.WithOptions = withOptions;
            this.Items = this.Columns.Count();

            if (associateIndexToKeyspace)
            {
                this.Keyspace.AssociateItem(this);
            }
        }

        #region IParsed
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this.Table.Cluster; } }
        public IDataCenter DataCenter { get { return this.Node?.DataCenter ?? this.Table.DataCenter; } }
        public INode Node { get; private set; }
        public int Items { get; private set; }
        public uint LineNbr { get; private set; }

        #endregion

        #region IDDLStmt
        public IKeyspace Keyspace { get { return this.Table.Keyspace; } }
        public string Name { get; private set; }
        public string DDL { get; private set; }
        public object ToDump()
        {
            return new
            {
                this.Keyspace,
                this.Table,
                this.Name,
                this.Columns,
                this.UsingClass,
                this.WithOptions,
                this.DDL
            };
        }
        public bool Equals(string other)
        {
            return this.Name == other;
        }

        #endregion

        #region ICQLIndex
        public bool IsCustom { get; private set; }
        public ICQLTable Table { get; private set; }
        public IEnumerable<CQLFunctionColumn> Columns { get; private set; }
        public string UsingClass { get; private set; }
        public Dictionary<string, object> WithOptions { get; private set; }
        #endregion

        #region IEquatable

        public bool Equals(ICQLIndex other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.DDL == other.DDL;
        }
        public bool Equals(ICQLTable other)
        {
            return this.Table.Equals(other);
        }
        #endregion

        #region overrides
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;
            if (obj is string) return this.Equals((string)obj);
            if (obj is IKeyspace) return this.Keyspace.Equals((IKeyspace)obj);
            if (obj is IDataCenter) return this.DataCenter.Equals((IDataCenter)obj);
            if (obj is ICQLTable) return this.Equals((ICQLTable)obj);
            if (obj is ICQLIndex) return this.Equals((ICQLIndex)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return (this.Keyspace.Name + "." + this.Name).GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("CQLIndex{{{0}}}", this.DDL);
        }
        #endregion
    }
}
