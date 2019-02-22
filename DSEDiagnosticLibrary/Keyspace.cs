using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using Common.Path;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticLibrary
{
    [JsonObject(MemberSerialization.OptOut)]
	public sealed class ColumnStats
    {
        public uint Collections;
        public uint Blobs;
        public uint Static;
        public uint Frozen;
        public uint Tuple;
        public uint UDT;
    }

    [JsonObject(MemberSerialization.OptOut)]
	public sealed class KeyspaceStats
	{
        public KeyspaceStats()
        {
            this.ColumnStat = new ColumnStats();
        }

		public uint Tables;
        public uint Columns;
        public ColumnStats ColumnStat;
        public uint SecondaryIndexes;
        public uint UserDefinedTypes;
        public uint MaterialViews;
        public uint Triggers;
        public uint Functions;
        public uint SolrIndexes;
        public uint SasIIIndexes;
        public uint CustomIndexes;
        public uint NbrObjects;
        public uint NbrActive;
        /// <summary>
        /// SizeTieredCompactionStrategy
        /// </summary>
        public uint STCS;
        /// <summary>
        /// LeveledCompactionStrategy
        /// </summary>
        public uint LCS;
        /// <summary>
        /// DateTieredCompactionStrategy
        /// </summary>
        public uint DTCS;
        /// <summary>
        /// TieredCompactionStrategy
        /// </summary>
        public uint TCS;
        /// <summary>
        /// TimeWindowCompactionStrategy
        /// </summary>
        public uint TWCS;
        public uint OtherStrategies;

        public TimeSpan MaxGCGrace = TimeSpan.MinValue;
        public TimeSpan MaxTTL = TimeSpan.MinValue;

        public decimal MaxReadRepairChance = decimal.MinValue;
        public decimal MaxReadRepairDCChance = decimal.MinValue;

        public uint NbrOrderBys;
        public uint NbrCompactStorage;

        public UnitOfMeasure Storage = UnitOfMeasure.NaNValue;
        public long ReadCount;
        public long WriteCount;
        public long KeyCount;
        public long SSTableCount;
    }

    [JsonObject(MemberSerialization.OptOut)]
	public sealed class KeyspaceReplicationInfo : IEquatable<IDataCenter>, IEquatable<string>, IEquatable<KeyspaceReplicationInfo>
    {
        public KeyspaceReplicationInfo(int replicationFactor, IDataCenter datacenter)
        {
            this.RF = (ushort)replicationFactor;
            this.DataCenter = datacenter;
        }

        public ushort RF { get; }
        public IDataCenter DataCenter { get; }

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

	public interface IKeyspace : IDDL, IEquatable<string>, IEquatable<IKeyspace>
    {
		IEnumerable<IDDL> DDLs { get; }
		string ReplicationStrategy { get; }
		KeyspaceStats Stats { get; }
		IEnumerable<IDataCenter> DataCenters { get; }
        IEnumerable<KeyspaceReplicationInfo> Replications { get; }
        bool DurableWrites { get; }
        bool EverywhereStrategy { get; }
        bool LocalStrategy { get; }
        IDDL AssociateItem(IDDL ddlItem);
        ICQLTable TryGetTable(string tableName);
        ICQLMaterializedView TryGetView(string viewName);
        ICQLIndex TryGetIndex(string indexName);
        ICQLTrigger TryGetTrigger(string triggerName);
        ICQLFunction TryGetFunction(string functionName);
        ICQLUserDefinedType TryGetUDT(string udtName);
        IDDLStmt TryGetDDL(string ddlName);
        IDDLStmt TryGetDDL(int ddlHashCode);
        IEnumerable<ICQLTable> GetTables();
        IEnumerable<ICQLUserDefinedType> GetUDTs();
        IEnumerable<ICQLIndex> GetIndexes();
        IEnumerable<ICQLTrigger> GetTriggers();
        IEnumerable<ICQLFunction> GetFunctions();
        IEnumerable<ICQLMaterializedView> GetViews();

        bool IsSystemKeyspace { get; }
        bool IsDSEKeyspace { get; }
        bool IsPerformanceKeyspace { get; }
        /// <summary>
        /// If true this keyspace was generated by the DSE Diagnostics library
        /// </summary>
        bool IsPreLoaded { get; }

        bool? HasActiveMember { get; }        
    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class KeySpace : IKeyspace
    {
        public KeySpace(IFilePath cqlFile,
                            uint lineNbr,
                            string name,
                            string replicationStrategy,
                            IEnumerable<KeyspaceReplicationInfo> replications,
                            bool durableWrites,
                            string ddl,
                            INode defindingNode,
                            Cluster cluster,
                            bool isPreLoaded)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException(string.Format("CQLKeyspace name cannot be null DDL: {0}", ddl));
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException(string.Format("CQLKeyspace must have a DDL string for Name {0}", name));

            this.IsPreLoaded = isPreLoaded;
            this.Stats = new KeyspaceStats();
            this.Path = cqlFile;
            this.Node = defindingNode;
            this._cluster = cluster;
            this.LineNbr = lineNbr;
            this.Name = StringHelpers.RemoveQuotes(name.Trim());
            this.ReplicationStrategy = StringHelpers.RemoveNamespace(replicationStrategy);
            this.DurableWrites = durableWrites;
            this.DDL = ddl;
            this.Replications = replications;
            this.EverywhereStrategy = this.ReplicationStrategy.ToUpper() == "EVERYWHERESTRATEGY";
            this.LocalStrategy = this.ReplicationStrategy.ToUpper() == "LOCALSTRATEGY";
            this.IsDSEKeyspace = LibrarySettings.DSEKeyspaces.Contains(this.Name);
            this.IsSystemKeyspace = LibrarySettings.SystemKeyspaces.Contains(this.Name);
            this.IsPerformanceKeyspace = LibrarySettings.PerformanceKeyspaces.Contains(this.Name);
            
            if (this.Replications == null || this.Replications.IsEmpty())
            {
                if (this.EverywhereStrategy || this.LocalStrategy)
                {
                    if(this.Replications == null)
                    {
                        this.Replications = Enumerable.Empty<KeyspaceReplicationInfo>();
                    }
                }
                else
                {                    
                    if (this.Node == null)
                    {
                        this.Replications = Enumerable.Empty<KeyspaceReplicationInfo>();
                        this.LocalStrategy = true;
                    }
                    else
                    {
                        this.Replications = new KeyspaceReplicationInfo[] { new KeyspaceReplicationInfo(1, this.Node.DataCenter) };
                    }                                                           
                }
            }
            else
            {
                this.Replications = this.Replications.OrderBy(r => r.DataCenter?.Name).ThenBy(r => r.RF);
            }

            if(this.Node == null
                    && (this._cluster == null || this._cluster.IsMaster))                    
            {
                if (this.Replications?.HasAtLeastOneElement() ?? false)
                {
                    this._cluster = this.Replications.First().DataCenter?.Cluster;
                }
                else
                {
                    this._cluster = Cluster.GetCurrentOrMaster();
                }
            }
        }

        #region IParse
        [JsonIgnore]
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
        public IPath Path { get; }
        [JsonProperty(PropertyName="Cluster")]
        private Cluster _cluster;
        [JsonIgnore]
        public Cluster Cluster { get { return this.Node == null ? this._cluster : this.Node.Cluster; } }
        [JsonIgnore]
        public IDataCenter DataCenter { get { return this.Node?.DataCenter ?? this.Replications.FirstOrDefault()?.DataCenter; } }
        public INode Node { get; }
        [JsonIgnore]
        public int Items { get { return this._ddlList.Count; } }
        public uint LineNbr { get; }
        #endregion

        #region IDDL
        public string Name { get; }
        [JsonIgnore]
        public string ReferenceName { get { return this.Name; } }
        [JsonIgnore]
        public string FullName { get { return this.Name; } }

        public string NameWAttrs(bool fullName = false, bool forceAttr = false)
        {
            return fullName ? this.FullName : this.Name;
        }
        public string ReferenceNameWAttrs(bool forceAttr = false)
        {
            return this.ReferenceName;
        }

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

        public object ToDump()
        {
            return new { KeySpace = this.Name, Cluster = this.Cluster.Name, DataCenter = this.DataCenter.Name, Me = this };
        }
        #endregion

        #region IKeyspace
        [JsonProperty(PropertyName="DDLs")]
        private IEnumerable<IDDL> datamemberDDLs
        {
            get { return this._ddlList.UnSafe; }
            set { this._ddlList = new CTS.List<IDDL>(value); }
        }
        private CTS.List<IDDL> _ddlList = new CTS.List<IDDL>();
        [JsonIgnore]
        public IEnumerable<IDDL> DDLs { get { return this._ddlList; } }
        public string ReplicationStrategy { get; }
        public bool EverywhereStrategy { get; }
        public bool LocalStrategy { get; }
        public KeyspaceStats Stats { get; }
        [JsonIgnore]
        public IEnumerable<IDataCenter> DataCenters { get { return this.EverywhereStrategy || this.LocalStrategy ? this.Cluster?.DataCenters : this.Replications.Select(r => r.DataCenter); } }
        public IEnumerable<KeyspaceReplicationInfo> Replications { get; }
        public bool DurableWrites { get; }

        [JsonIgnore]
        public bool? HasActiveMember
        {
            get
            {
                var isActive = this._ddlList.Where(c => c is IDDLStmt).Select(c =>  ((IDDLStmt)c).IsActive);
                return isActive.IsEmpty() || isActive.All(i => !i.HasValue)
                        ? (bool?) null
                        : isActive.Any(i => i.HasValue && i.Value);
            }
        }
        public IDDL AssociateItem(IDDL ddl)
        {
            this._ddlList.Lock();
            try
            {
                var item = this._ddlList.FirstOrDefault(d => d.Equals(ddl));

                if (item == null)
                {
                    this._ddlList.UnSafe.Add(ddl);
                }
                else
                {
                    return item;
                }
            }
            finally
            {
                this._ddlList.UnLock();
            }

            ++this.Stats.NbrObjects;

            if (ddl is ICQLTable)
            {
                if (ddl is ICQLMaterializedView)
                    ++this.Stats.MaterialViews;
                else
                    ++this.Stats.Tables;

                if (!string.IsNullOrEmpty(((ICQLTable)ddl).Compaction))
                {
                    switch (((ICQLTable)ddl).Compaction)
                    {
                        case "SizeTieredCompactionStrategy":
                            ++this.Stats.STCS;
                            break;
                        case "DateTieredCompactionStrategy":
                            ++this.Stats.DTCS;
                            break;
                        case "LeveledCompactionStrategy":
                            ++this.Stats.LCS;
                            break;
                        case "TimeWindowCompactionStrategy":
                            ++this.Stats.TWCS;
                            break;
                        case "TieredCompactionStrategy":
                            ++this.Stats.TCS;
                            break;
                        default:
                            ++this.Stats.OtherStrategies;
                            break;
                    }
                }

                this.Stats.Columns += ((ICQLTable)ddl).Stats.NbrColumns;
                this.Stats.ColumnStat.Blobs += ((ICQLTable)ddl).Stats.Blobs;
                this.Stats.ColumnStat.Collections += ((ICQLTable)ddl).Stats.Collections;
                this.Stats.ColumnStat.Frozen += ((ICQLTable)ddl).Stats.Frozens;
                this.Stats.ColumnStat.Static += ((ICQLTable)ddl).Stats.Statics;
                this.Stats.ColumnStat.Tuple += ((ICQLTable)ddl).Stats.Tuples;
                this.Stats.ColumnStat.UDT += ((ICQLTable)ddl).Stats.UDTs;
                this.Stats.MaxGCGrace = this.Stats.MaxGCGrace.Max((TimeSpan)(((ICQLTable)ddl).GetPropertyValue("gc_grace_seconds") ?? TimeSpan.MinValue));
                this.Stats.MaxTTL = this.Stats.MaxTTL.Max((TimeSpan)(((ICQLTable)ddl).GetPropertyValue("default_time_to_live") ?? TimeSpan.MinValue));
                this.Stats.MaxReadRepairChance = Math.Max(this.Stats.MaxReadRepairChance, (decimal)(((ICQLTable)ddl).GetPropertyValue("read_repair_chance") ?? decimal.MinValue));
                this.Stats.MaxReadRepairDCChance = Math.Max(this.Stats.MaxReadRepairDCChance, (decimal)(((ICQLTable)ddl).GetPropertyValue("dclocal_read_repair_chance") ?? decimal.MinValue));

                {
                    var compactStorage = ((ICQLTable)ddl).GetPropertyValue("compact storage");
                    if (compactStorage != null && (bool)compactStorage)
                        ++this.Stats.NbrCompactStorage;
                }

                if (((ICQLTable)ddl).OrderByCols.HasAtLeastOneElement())
                    ++this.Stats.NbrOrderBys;
            }
            else if (ddl is ICQLUserDefinedType)
            {
                ++this.Stats.UserDefinedTypes;
            }
            else if (ddl is ICQLIndex)
            {
                var idxInstance = (ICQLIndex)ddl;

                if (idxInstance.IsCustom)
                {
                    if (idxInstance.IsSolr)
                    {
                        ++this.Stats.SolrIndexes;
                    }
                    else if (idxInstance.IsSasII)
                    {
                        ++this.Stats.SasIIIndexes;
                    }
                    else
                    {
                        ++this.Stats.CustomIndexes;
                    }
                }
                else
                {
                    ++this.Stats.SecondaryIndexes;
                }
            }
            else if (ddl is ICQLTrigger)
            {
                ++this.Stats.Triggers;
            }
            else if (ddl is ICQLFunction)
            {
                ++this.Stats.Functions;
            }

            return ddl;
        }

        public ICQLTable TryGetTable(string tableName)
        {
            tableName = StringHelpers.RemoveQuotes(tableName.Trim());

            this._ddlList.Lock();
            try
            {
                return (ICQLTable)this._ddlList.UnSafe.FirstOrDefault(d => d is ICQLTable && !(d is ICQLMaterializedView) && d.Name == tableName);
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }

        public ICQLMaterializedView TryGetView(string viewName)
        {
            viewName = StringHelpers.RemoveQuotes(viewName.Trim());

            this._ddlList.Lock();
            try
            {
                return (ICQLMaterializedView)this._ddlList.UnSafe.FirstOrDefault(d => d is ICQLMaterializedView && d.Name == viewName);
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }

        public ICQLIndex TryGetIndex(string indexName)
        {
            indexName = StringHelpers.RemoveQuotes(indexName.Trim());

            this._ddlList.Lock();
            try
            {
                return (ICQLIndex)this._ddlList.UnSafe.FirstOrDefault(d => d is ICQLIndex && (d.Name == indexName || d.ReferenceName == indexName) );
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }

        public ICQLTrigger TryGetTrigger(string triggerName)
        {
            triggerName = StringHelpers.RemoveQuotes(triggerName.Trim());

            this._ddlList.Lock();
            try
            {
                return (ICQLTrigger)this._ddlList.UnSafe.FirstOrDefault(d => d is ICQLTrigger && d.Name == triggerName);
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }

        public ICQLFunction TryGetFunction(string functionName)
        {
            functionName = StringHelpers.RemoveQuotes(functionName.Trim());

            this._ddlList.Lock();
            try
            {
                return (ICQLFunction)this._ddlList.UnSafe.FirstOrDefault(d => d is ICQLTrigger && d.Name == functionName);
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }

        public IEnumerable<ICQLTable> GetTables()
        {
            return this._ddlList.Where(c => c is ICQLTable && !(c is ICQLMaterializedView)).Cast<ICQLTable>();
        }

        public IEnumerable<ICQLMaterializedView> GetViews()
        {
            return this._ddlList.Where(c => c is ICQLMaterializedView).Cast<ICQLMaterializedView>();
        }

        public IEnumerable<ICQLIndex> GetIndexes()
        {
            return this._ddlList.Where(c => c is ICQLIndex).Cast<ICQLIndex>();
        }

        public IEnumerable<ICQLTrigger> GetTriggers()
        {
            return this._ddlList.Where(c => c is ICQLTrigger).Cast<ICQLTrigger>();
        }

        public IEnumerable<ICQLFunction> GetFunctions()
        {
            return this._ddlList.Where(c => c is ICQLFunction).Cast<ICQLFunction>();
        }

        public ICQLUserDefinedType TryGetUDT(string udtName)
        {
            udtName = StringHelpers.RemoveQuotes(udtName.Trim());

            this._ddlList.Lock();
            try
            {
                return (ICQLUserDefinedType)this._ddlList.UnSafe.FirstOrDefault(d => d is ICQLUserDefinedType && d.Name == udtName);
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }

        public IDDLStmt TryGetDDL(string ddlName)
        {
            ddlName = StringHelpers.RemoveQuotes(ddlName.Trim());

            this._ddlList.Lock();
            try
            {
                return (IDDLStmt)this._ddlList
                                        .UnSafe
                                        .FirstOrDefault(d => ((d is ICQLTable
                                                                    || d is ICQLMaterializedView
                                                                    || d is ICQLTrigger
                                                                    || d is ICQLFunction)
                                                                && d.Name == ddlName)
                                                              || (d is ICQLIndex
                                                                    && (d.Name == ddlName || d.ReferenceName == ddlName)));
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }
        public IDDLStmt TryGetDDL(int ddlHashCode)
        {
            this._ddlList.Lock();
            try
            {
                return (IDDLStmt)this._ddlList
                                        .UnSafe
                                        .FirstOrDefault(d => d.GetHashCode() == ddlHashCode);
            }
            finally
            {
                this._ddlList.UnLock();
            }
        }
        public IEnumerable<ICQLUserDefinedType> GetUDTs()
        {
            return this._ddlList.Where(c => c is ICQLUserDefinedType).Cast<ICQLUserDefinedType>();
        }

        public bool IsSystemKeyspace { get; }
        public bool IsDSEKeyspace { get; }
        public bool IsPerformanceKeyspace { get; }

        public bool IsPreLoaded { get; }

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
                    && this.DurableWrites == other.DurableWrites)
            {
                if (this.EverywhereStrategy && other.EverywhereStrategy)
                {
                    return true;
                }

                if (this.LocalStrategy && other.LocalStrategy)
                {
                    return true;
                }

                if (!this.EverywhereStrategy && !other.EverywhereStrategy
                        && !this.LocalStrategy && !other.LocalStrategy
                        && this.Replications.Count() == other.Replications.Count())
                {
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

        [JsonProperty(PropertyName="HashCode")]
        private int _hashcode = 0;
        public override int GetHashCode()
        {
            unchecked
            {
                if (this._hashcode != 0) return this._hashcode;
                if (this.Cluster.IsMaster) return this.FullName.GetHashCode();

                return this._hashcode = (589 + this.Cluster.GetHashCode()) * 31 + this.FullName.GetHashCode();
            }
        }

        public override string ToString()
        {
            return string.Format("Keyspace{{{0}}}", this.DDL);
        }
        #endregion

        public UnitOfMeasure AddToStorage(UnitOfMeasure size)
        {
            return this.Stats.Storage = this.Stats.Storage.Add(size);
        }
        public UnitOfMeasure AddToStorage(decimal size)
        {
            return this.Stats.Storage = this.Stats.Storage.Add(size);
        }
        public long AddToReadCount(long readCount)
        {
            return this.Stats.ReadCount += readCount;
        }
        public long AddToWriteCount(long writeCount)
        {
            return this.Stats.WriteCount += writeCount;
        }
        public long AddToKeyCount(long keyCount)
        {
            return this.Stats.KeyCount += keyCount;
        }
        public long AddToSSTableCount(long ssTableCount)
        {
            return this.Stats.SSTableCount += ssTableCount;
        }

        public static IEnumerable<IKeyspace> TryGet(Cluster cluster, string ksName)
        {
            return cluster.GetKeyspaces(StringHelpers.RemoveQuotes(ksName.Trim()));
        }
        public static IEnumerable<IKeyspace> TryGet(IDataCenter datacenter, string ksName)
        {
            ksName = StringHelpers.RemoveQuotes(ksName.Trim());
            return datacenter.Keyspaces.Where(k => k.Name == ksName);
        }

    }


}
