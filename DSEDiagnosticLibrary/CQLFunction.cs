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
    public interface ICQLFunction : IDDLStmt, IEquatable<string>, IEquatable<ICQLFunction>
    {
        string InputAction { get; }
        IEnumerable<ICQLColumn> Inputs { get; }
        CQLColumnType ReturnType { get; }
        string Language{ get; }
        string CodeBlock { get; }
    }

    public class CQLFunction : ICQLFunction
    {
        public CQLFunction(IFilePath cqlFile,
                            uint lineNbr,
                            string name,
                            IKeyspace keyspace,
                            string inputAction,
                            IEnumerable<ICQLColumn> inputs,
                            CQLColumnType returnType,
                            string language,
                            string codeBlock,
                            string ddl,
                            INode defindingNode = null,
                            bool associateTriggerToKeyspace = true)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException(string.Format("CQLFunction name cannot be null for CQL \"{0}\"", ddl));
            if (keyspace == null) throw new NullReferenceException(string.Format("CQLFunction must be associated to a CQL Keyspace. It cannot be null for CQL \"{0}\"", ddl));
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException(string.Format("CQLFunction \"{0}\" must have a DDL string", name));

            this.Path = cqlFile;
            this.Keyspace = keyspace;
            this.Node = defindingNode ?? keyspace.Node;
            this.LineNbr = lineNbr;
            this.Name = StringHelpers.RemoveQuotes(name.Trim());
            this.InputAction = inputAction;
            this.ReturnType = returnType;
            this.CodeBlock = codeBlock?.Trim();
            this.DDL = ddl;
            this.Language = StringHelpers.RemoveQuotes(language.Trim());
            this.Items = 1;

            if (associateTriggerToKeyspace)
            {
                this.Keyspace.AssociateItem(this);
            }
            
        }

        #region IParsed
        [JsonIgnore]
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        [JsonConverter(typeof(IPathJsonConverter))]
        public IPath Path { get; }
        [JsonIgnore]
        public Cluster Cluster { get { return this.Keyspace.Cluster; } }
        [JsonIgnore]
        public IDataCenter DataCenter { get { return this.Keyspace.DataCenter; } }
        public INode Node { get; }
        public int Items { get; }
        public uint LineNbr { get; }

        #endregion

        #region IDDLStmt
        
        public IKeyspace Keyspace { get; }
        public string Name { get; }
        [JsonIgnore]
        public string ReferenceName { get { return this.Keyspace.Name + '.' + this.Name; } }
        [JsonIgnore]
        public string FullName { get { return this.Keyspace.FullName + '.' + this.Name; } }
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

        public string NameWAttrs(bool fullName = false, bool forceAttr = false)
        {
            return fullName ? this.FullName : this.Name;
        }

        public string ReferenceNameWAttrs(bool forceAttr = false)
        {
            return this.ReferenceName;
        }

        public object ToDump()
        {
            return new { Function = this.FullName, Cluster = this.Cluster.Name, DataCenter = this.DataCenter.Name, Me = this };
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

        #region ICQLFunction

        public string InputAction { get; }
        public IEnumerable<ICQLColumn> Inputs { get; }
        public CQLColumnType ReturnType { get; }
        public string Language { get; }
        public string CodeBlock { get; }

        #endregion

        #region IEquatable

        public bool Equals(ICQLFunction other)
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
            if (obj is ICQLFunction) return this.Equals((ICQLFunction)obj);

            return base.Equals(obj);
        }

        [JsonProperty(PropertyName = "HashCode")]
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
            return string.Format("CQLFunction{{{0}}}", this.DDL);
        }
        #endregion

    }
}
