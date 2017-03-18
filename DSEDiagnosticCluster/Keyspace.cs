using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Path;

namespace DSEDiagnosticLibrary
{
	public sealed class KeyspaceStats
	{
		uint Tables;
		uint Indexes;
        uint MaterialViews;
		uint SolrIndexes;
		uint NbrObjects;
		uint NbrActive;
		uint STCS;
		uint LCS;
		uint DTCS;
		uint TCS;
		uint TWCS;
		uint OtherStrategies;
	}

	public sealed class KeyspaceReplicationInfo : IEquatable<IDataCenter>, IEquatable<string>, IEquatable<KeyspaceReplicationInfo>
    {
        public KeyspaceReplicationInfo(int replicationFactor, IDataCenter datacenter)
        {
            this.RF = (ushort)replicationFactor;
            this.DataCenter = datacenter;
        }

        public ushort RF { get; private set; }
		public IDataCenter DataCenter { get; private set; }

        public bool Equals(IDataCenter other)
        {
            return this.DataCenter == null ? other == null : this.DataCenter.Equals(other);
        }

        public bool Equals(string other)
        {
            return this.DataCenter == null ? other == null : this.DataCenter.Equals(other);
        }

        public bool Equals(KeyspaceReplicationInfo other)
        {
            if(this.DataCenter == null && other.DataCenter == null)
            {
                return this.RF == other.RF;
            }

            return this.DataCenter.Equals(other.DataCenter) && this.RF == other.RF;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, this)) return true;
            if(obj is string) return this.Equals((string)obj);
            if (obj is IDataCenter) return this.Equals((IDataCenter)obj);
            if(obj is KeyspaceReplicationInfo) return this.Equals((KeyspaceReplicationInfo)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.DataCenter.GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("{0}:{1}", this.DataCenter?.Name, this.RF);
        }

        public object ToDump()
        {
            return new { DataCenter = this?.DataCenter, RF = this.RF };
        }
    }

	public interface IKeyspace : IDDL, IEquatable<string>
	{
		IEnumerable<IDDL> DDLs { get; }
		string ReplicationStrategy { get; }
		KeyspaceStats Stats { get; }
		IEnumerable<IDataCenter> DataCenters { get; }
        IEnumerable<KeyspaceReplicationInfo> Replications { get; }
        bool DurableWrites { get; }
        bool EverywhereStrategy { get; }
        IKeyspace AssociateItem(IDDL ddlItem);
    }

    public sealed class KeySpace : IKeyspace, IEquatable<IKeyspace>
    {
        public KeySpace(IFilePath cqlFile,
                            uint lineNbr,
                            string name,
                            string replicationStrategy,
                            IEnumerable<KeyspaceReplicationInfo> replications,
                            bool durableWrites,
                            string ddl,
                            INode defindingNode,
                            Cluster cluster)
        {
            this.Stats = new KeyspaceStats();
            this.Path = cqlFile;
            this.Node = defindingNode;
            this._cluster = cluster;
            this.LineNbr = lineNbr;
            this.Name = name;
            this.ReplicationStrategy = replicationStrategy;
            this.DurableWrites = durableWrites;
            this.DDL = ddl;
            this.Replications = replications;
            this.EverywhereStrategy = this.ReplicationStrategy.ToUpper() == "EVERYWHERESTRATEGY";

            if (this.Replications == null || this.Replications.Count() == 0)
            {
                if (this.EverywhereStrategy)
                {
                    if(this.Replications == null)
                    {
                        this.Replications = Enumerable.Empty<KeyspaceReplicationInfo>();
                    }
                }
                else
                {
                    throw new ArgumentException(string.Format("Replication Factors must be defined for keyspace \"{0}\". Either null or zero RFs were passed to this constructor.", name),
                                                    "replications");
                }
            }
            else
            {
                this.Replications = this.Replications.OrderBy(r => r.DataCenter?.Name).ThenBy(r => r.RF);
            }
        }

        #region IParse
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        public IPath Path { get; private set; }
        private Cluster _cluster;
        public Cluster Cluster { get { return this.Node == null ? this._cluster : this.Node.Cluster; } }
        public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
        public INode Node { get; private set; }
        public int Items { get { return this._ddlList.Count; } }
        public uint LineNbr { get; private set; }
        #endregion

        #region IDDL
        public string Name { get; private set; }
        public string DDL { get; private set; }
        public object ToDump()
        {
            return new { Name = this.Name, ReplicationStrategy = this.ReplicationStrategy, Replications = this.Replications, DDL = this.DDL, DDLs=this.DDLs };
        }
        #endregion

        #region IKeyspace
        private List<IDDL> _ddlList = new List<IDDL>();
        public IEnumerable<IDDL> DDLs { get { return this._ddlList; } }
        public string ReplicationStrategy { get; private set; }
        public bool EverywhereStrategy { get; private set; }
        public KeyspaceStats Stats { get; private set; }
        public IEnumerable<IDataCenter> DataCenters { get { return this.EverywhereStrategy ? this.Cluster?.DataCenters : this.Replications.Select(r => r.DataCenter); } }
        public IEnumerable<KeyspaceReplicationInfo> Replications { get; private set; }
        public bool DurableWrites { get; private set; }
        public IKeyspace AssociateItem(IDDL ddl)
        {
            //Add Stats based on object type
            this._ddlList.Add(ddl);
            return this;
        }
        #endregion

        #region IEquatable
        public bool Equals(string other)
        {
            return this.Name == other;
        }

        public bool Equals(IKeyspace other)
        {
            if(this.Name == other.Name
                    && this.ReplicationStrategy == other.ReplicationStrategy
                    && this.DurableWrites == other.DurableWrites
                    && this.Replications.Count() == other.Replications.Count())
            {
                if(this.EverywhereStrategy == other.EverywhereStrategy)
                {
                    if(this.EverywhereStrategy)
                    {
                        return true;
                    }

                    return this.Replications
                                .SelectWithIndex((r, idx) => r.Equals(other.Replications.ElementAt(idx)))
                                .All(b => b);
                }
            }

            return false;
        }

        #endregion

        #region overrides
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;
            if (obj is string) return this.Equals((string)obj);
            if (obj is IKeyspace) return this.Equals((IKeyspace)obj);
            if (obj is IDataCenter) return this.DataCenters.Contains((IDataCenter)obj);
            if (obj is KeyspaceReplicationInfo) return this.Replications.Contains((KeyspaceReplicationInfo)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("Keyspace{{{0}}}", this.DDL);
        }
        #endregion

    }


}
