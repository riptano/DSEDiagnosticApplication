using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticLibrary
{
    public interface ICQLTrigger : IDDLStmt, IEquatable<string>, IEquatable<ICQLTrigger>, IEquatable<ICQLTable>
    {
        ICQLTable Table { get; }
        string JavaClass { get; }
    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class CQLTrigger : ICQLTrigger
    {
        public CQLTrigger(IFilePath cqlFile,
                            uint lineNbr,
                            string name,
                            ICQLTable table,
                            string javaClass,
                            string ddl,
                            INode defindingNode = null,
                            bool associateTriggerToKeyspace = true,
                            bool associateTriggerToTable = true)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException(string.Format("CQLTrigger name cannot be null for CQL \"{0}\"", ddl));
            if (table == null) throw new NullReferenceException(string.Format("CQLTrigger must be associated to a CQL Table. It cannot be null for CQL \"{0}\"", ddl));
            if (string.IsNullOrEmpty(javaClass)) throw new NullReferenceException(string.Format("CQLTrigger must have a Java Class for CQL \"{0}\"", ddl));
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException(string.Format("CQLTrigger \"{0}\" must have a DDL string", name));

            this.Path = cqlFile;
            this.Table = table;
            this.Node = defindingNode ?? table.Node;
            this.LineNbr = lineNbr;
            this.Name = StringHelpers.RemoveQuotes(name.Trim());
            this.DDL = ddl;
            this.JavaClass = StringHelpers.RemoveQuotes(javaClass.Trim());
            this.Items = 1;

            if (associateTriggerToKeyspace)
            {
                this.Keyspace.AssociateItem(this);
            }
            if (associateTriggerToTable)
            {
                this.Table.AssociateItem(this);
            }
        }

        #region IParsed
        [JsonIgnore]
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        [JsonConverter(typeof(IPathJsonConverter))]
        public IPath Path { get;}
        [JsonIgnore]
        public Cluster Cluster { get { return this.Table.Cluster; } }
        [JsonIgnore]
        public IDataCenter DataCenter { get { return this.Node?.DataCenter ?? this.Table.DataCenter; } }
        public INode Node { get; }
        public int Items { get;  }
        public uint LineNbr { get; }

        #endregion

        #region IDDLStmt
        [JsonIgnore]
        public IKeyspace Keyspace { get { return this.Table.Keyspace; } }
        public string Name { get;  }
        [JsonIgnore]
        public string ReferenceName { get { return this.Table.Name + '.' + this.Name; } }
        [JsonIgnore]
        public string FullName { get { return this.Table.FullName + '.' + this.Name; } }
        public string DDL { get; }

        [JsonProperty(PropertyName = "AggregatedStats")]
        private IEnumerable<IAggregatedStats> datamemberAggregatedStats
        {
            get { return this._aggregatedStats.UnSafe; }
            set { this._aggregatedStats = new CTS.List<IAggregatedStats>(value); }
        }

        private CTS.List<IAggregatedStats> _aggregatedStats = new CTS.List<IAggregatedStats>();
        [JsonIgnore]
        public IEnumerable<IAggregatedStats> AggregatedStats { get { return this._aggregatedStats; } }
        public IDDL AssociateItem(IAggregatedStats aggregatedStat)
        {
            this._aggregatedStats.Add(aggregatedStat);
            return this;
        }

        public bool? IsActive { get; private set; }
        public bool? SetAsActive(bool isActive = true)
        {
            if (!this.IsActive.HasValue || isActive != this.IsActive.Value)
            {
                this.IsActive = isActive;
            }
            return this.IsActive;
        }

        public object ToDump()
        {
            return new { Trigger = this.FullName, Cluster = this.Cluster.Name, DataCenter = this.DataCenter.Name, Me = this };
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

            return this.Name == kstblpair.Item2 || this.ReferenceName == kstblpair.Item2;
        }

        #endregion

        #region ICQLTrigger
        public ICQLTable Table { get; }
        public string JavaClass { get; }
        #endregion

        #region IEquatable

        public bool Equals(ICQLTrigger other)
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
            if (obj is ICQLTrigger) return this.Equals((ICQLTrigger)obj);

            return base.Equals(obj);
        }

        [JsonProperty(PropertyName="HashCode")]
        private int _hashcode = 0;
        public override int GetHashCode()
        {
            unchecked
            {
                if (this._hashcode != 0) return this._hashcode;
                if (this.Keyspace.Cluster.IsMaster) return this.FullName.GetHashCode();

                return this._hashcode = this.Table.GetHashCode() * 31 + (this.JavaClass + '.' + this.Name).GetHashCode();
            }
        }

        public override string ToString()
        {
            return string.Format("CQLTrigger{{{0}}}", this.DDL);
        }
        #endregion
    }
}
