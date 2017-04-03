using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{

    public interface ICQLIndex : IDDLStmt, IEquatable<string>, IEquatable<ICQLIndex>, IEquatable<ICQLTable>
    {
        bool IsCustom { get; }
        bool IsSolr { get; }
        bool IsSasII { get; }
        ICQLTable Table { get; }
        IEnumerable<CQLFunctionColumn> Columns { get; }
        string UsingClass { get; }
        Dictionary<string,object> WithOptions { get; }
    }

    public sealed class CQLIndex : ICQLIndex
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
                        bool associateIndexToKeyspace = true,
                        bool associateIndexToTable = true)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException(string.Format("CQLIndex name cannot be null for CQL \"{0}\"", ddl));
            if (table == null) throw new NullReferenceException(string.Format("CQLIndex must be associated to a CQL Table. It cannot be null for CQL \"{0}\"", ddl));
            if (columns == null || columns.IsEmpty()) throw new NullReferenceException(string.Format("CQLIndex must have columns (cannot be null or a count of zero) for CQL \"{0}\"", ddl));
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException(string.Format("CQLIndex \"{0}\" must have a DDL string", name));

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

            if (this.IsCustom && this.UsingClass == null) throw new NullReferenceException(string.Format("CQLIndex \"{0}\" must have a usingClass string for custom index's for CQL \"{1}\"", name, ddl));

            if (associateIndexToKeyspace)
            {
                this.Keyspace.AssociateItem(this);
            }
            if(associateIndexToTable)
            {
                this.Table.AssociateItem(this);
            }

            if(this.IsCustom)
            {
                this.IsSolr = LibrarySettings.IsSolrIndexClass.Contains(this.UsingClass);
                if (!this.IsSolr)
                {
                    this.IsSasII = LibrarySettings.IsSasIIIndexClasses.Contains(this.UsingClass);
                }
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

        public string FullName { get { return this.Keyspace.Name + '.' + this.Name; } }
        public string DDL { get; private set; }
        public object ToDump()
        {
            return this;
        }
        public bool Equals(string other)
        {
            var kstblpair = StringHelpers.SplitTableName(other);

            if (kstblpair.Item1 != null)
            {
                if (!this.Keyspace.Equals(kstblpair.Item1))
                {
                    return false;
                }
            }

            return this.Name == kstblpair.Item2;
        }

        #endregion

        #region ICQLIndex
        public bool IsCustom { get; private set; }
        public bool IsSolr { get; private set; }
        public bool IsSasII { get; private set; }
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

        private int _hashcode = 0;
        public override int GetHashCode()
        {
            unchecked
            {
                if (this._hashcode != 0) return this._hashcode;
                if (this.Keyspace.Cluster.IsMaster) return this.FullName.GetHashCode();
                return this._hashcode = this.Keyspace.GetHashCode() * 31 + this.Name.GetHashCode();
            }           
        }

        public override string ToString()
        {
            return string.Format("CQLIndex{{{0}}}", this.DDL);
        }
        #endregion
    }
}
