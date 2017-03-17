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
        internal KeyspaceReplicationInfo(int replicationFactor, IDataCenter datacenter)
        {
            this.RF = (ushort)replicationFactor;
            this.DataCenter = datacenter;
        }

        public ushort RF { get; private set; }
		public IDataCenter DataCenter { get; private set; }

        public bool Equals(IDataCenter other)
        {
            return this.DataCenter.Equals(other);
        }

        public bool Equals(string other)
        {
            return this.DataCenter.Equals(other);
        }

        public bool Equals(KeyspaceReplicationInfo other)
        {
            return this.DataCenter.Equals(other.DataCenter) && this.RF == other.RF;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, this)) return true;
            if(obj is string) return this.Equals((string)obj);
            if (obj is IDataCenter) return this.Equals((IDataCenter)obj);
            if(obj is KeyspaceReplicationInfo) return this.DataCenter.Equals((KeyspaceReplicationInfo)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.DataCenter.GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("{0}:{1}", this.DataCenter.Name, this.RF);
        }

        public object ToDump()
        {
            return new { DataCenter = this.DataCenter, RF = this.RF };
        }
    }

	public interface IKeyspace : IDDL, IEquatable<string>
	{
		IEnumerable<IDDL> DDLs { get; }
		string ReplicationStrategy { get; }
		KeyspaceStats Stats { get; }
		IEnumerable<IDataCenter> DataCenters { get; }
        IEnumerable<KeyspaceReplicationInfo> Replications { get; }
        IKeyspace AssociateItem(IDDL ddlItem);
    }

    public sealed class KeySpace : IKeyspace, IEquatable<IKeyspace>
    {
        internal KeySpace(IFilePath cqlFile,
                            uint lineNbr,
                            string name,
                            string replicationStrategy,
                            IEnumerable<KeyspaceReplicationInfo> replications,
                            string ddl)
        {
            this.Stats = new KeyspaceStats();
            this.Path = cqlFile;
            this.LineNbr = lineNbr;
            this.Name = name;
            this.ReplicationStrategy = replicationStrategy;
            this.DDL = ddl;
            this.Replications = replications;

            if(this.Replications == null || this.Replications.Count() == 0)
            {
                throw new ArgumentException(string.Format("Replication Factors must be defined for keyspace \"{0}\". Either null or zero RFs were passed to this constructor.", name),
                                            "replications");
            }
        }

        #region IParse
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this.Replications.First().DataCenter.Cluster; } }
        public IDataCenter DataCenter { get { throw new NotImplementedException(); } }
        public INode Node { get { throw new NotImplementedException(); } }
        public int Items { get { return this._ddlList.Count; } }
        public uint LineNbr { get; private set; }
        #endregion

        #region IDDL
        public string Name { get; private set; }
        public string DDL { get; private set; }
        public object ToDump()
        {
            return new { Name = this.Name, Replications = this.Replications, DDL = this.DDL, DDLs=this.DDLs };
        }
        #endregion

        #region IKeyspace
        private List<IDDL> _ddlList = new List<IDDL>();
        public IEnumerable<IDDL> DDLs { get { return this._ddlList; } }
        public string ReplicationStrategy { get; private set; }
        public KeyspaceStats Stats { get; private set; }
        public IEnumerable<IDataCenter> DataCenters { get { return this.Replications.Select(r => r.DataCenter); } }
        public IEnumerable<KeyspaceReplicationInfo> Replications { get; private set; }

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
                    && this.Replications.Count() == other.Replications.Count())
            {
                return this.Replications.Union(other.Replications).Count() == this.Replications.Count();
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

        public IKeyspace TryAddKeyspace(Cluster cluster,
                                        IFilePath cqlFile,
                                        uint lineNbr,
                                        string ddl)
        {

            return null;
        }
    }


}
