using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public interface ICQLUserDefinedType : IDDLStmt, IEquatable<string>, IEquatable<ICQLUserDefinedType>
    {
        IEnumerable<ICQLColumn> Columns { get; }
    }

    public sealed class CQLUserDefinedType : ICQLUserDefinedType
    {
        public CQLUserDefinedType(IFilePath cqlFile,
                                    uint lineNbr,
                                    IKeyspace keyspace,
                                    string name,
                                    IEnumerable<ICQLColumn> columns,
                                    string ddl,
                                    INode defindingNode = null,
                                    bool associateUDTToColumn = true,
                                    bool associateUDTToKeyspace = true)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException("CQLUserDefinedType name cannot be null");
            if (keyspace == null) throw new NullReferenceException("CQLUserDefinedType must be associated to a keyspace. It cannot be null");
            if (columns == null || columns.IsEmpty()) throw new NullReferenceException("CQLUserDefinedType must have columns (cannot be null or a count of zero)");
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException("CQLUserDefinedType must have a DDL string");

            this.Path = cqlFile;
            this.Keyspace = keyspace;
            this.Node = defindingNode ?? keyspace.Node;
            this.LineNbr = lineNbr;
            this.Name = StringHelpers.RemoveQuotes(name.Trim());
            this.DDL = ddl;
            this.Columns = columns;
            this.Items = this.Columns.Count();

            if (associateUDTToColumn)
            {
                foreach (var col in this.Columns)
                {
                    ((CQLColumn)col).SetUDT(this);
                }
            }

            if (associateUDTToKeyspace)
            {
                this.Keyspace.AssociateItem(this);
            }
        }

        public IEnumerable<ICQLColumn> Columns { get; private set; }

        #region IParsed
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this.Keyspace.Cluster; } }
        public IDataCenter DataCenter { get { return this.Node?.DataCenter ?? this.Keyspace.DataCenter; } }
        public INode Node { get; private set; }
        public int Items { get; private set; }
        public uint LineNbr { get; private set; }

        #endregion

        #region IDDLStmt
        public IKeyspace Keyspace { get; private set; }
        public string Name { get; private set; }
        public string DDL { get; private set; }
        public object ToDump()
        {
            return this;
        }
        public bool Equals(string other)
        {
            return this.Name == other;
        }

        #endregion

        #region IEquatable<ICQLUserDefinedType>
        public bool Equals(ICQLUserDefinedType other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.DDL == other.DDL;
        }

        #endregion

        #region overrides
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;
            if (obj is string) return this.Equals((string)obj);
            if (obj is IKeyspace) return this.Keyspace.Equals((IKeyspace)obj);
            if (obj is IDataCenter) return this.DataCenter.Equals((IDataCenter)obj);
            if (obj is ICQLUserDefinedType) return this.Equals((ICQLUserDefinedType)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return (this.Keyspace.Name + "." + this.Name).GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("CQLUserDefinedType{{{0}}}", this.DDL);
        }
        #endregion

        public static ICQLUserDefinedType TryGet(IKeyspace ksInstance, string name)
        {
            return ksInstance.TryGetUDT(name);
        }
    }
}
