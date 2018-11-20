using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Newtonsoft.Json;
using Common;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticAnalytics
{
    public sealed class DataModelDCPK : AnalyzeData
    {
        static readonly IEnumerable<int> EmptyReconciliationRefs = Enumerable.Empty<int>();

        public DataModelDCPK(Cluster cluster, CancellationToken cancellationToken, string[] ignoreKeySpaces = null)
            : base(cluster, cancellationToken)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }

        #region IAggregatedStats

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class CommonPartitionKey : IAggregatedStats, IEquatable<IEnumerable<ICQLColumn>>, IEquatable<ICQLTable>
        {
            const string PKPropKey = "PartitionKey";
            const string TablesPropKey = "Tables";

            #region Constructors

            public CommonPartitionKey(IDataCenter dataCenter,
                                        IEnumerable<ICQLColumn> pks)
            {
                this.DataCenter = dataCenter;
                this._data.Add(PKPropKey, pks);
            }

            public CommonPartitionKey(Cluster cluster,
                                        IEnumerable<ICQLColumn> pks)
            {
                this._cluster = cluster;
                this._data.Add(PKPropKey, pks);
            }

            public CommonPartitionKey(ICQLTable assocTable)
                : this(assocTable.Cluster, assocTable.PrimaryKeys)
            {
                this.AssociateItem(assocTable);
            }

            #endregion

            #region IParsed Members

            public SourceTypes Source { get; } = SourceTypes.CQL;
            public IPath Path { get; } = null;

            private readonly Cluster _cluster;
            public Cluster Cluster { get { return this._cluster ?? this.DataCenter?.Cluster; } }
            public IDataCenter DataCenter { get; }
            public INode Node { get; } = null;
            public int Items { get; }
            public uint LineNbr { get; } = 0;

            #endregion

            #region IEvent

            public EventTypes Type { get; } = EventTypes.AggregateDataDerived;
            public EventClasses Class { get; } = EventClasses.DataModelStats;
            public string SubClass { get; } = "Common Partition Key";

            public Guid Id { get; } = Guid.Empty;

            [JsonIgnore]
            public IEnumerable<IEvent> ParentEvents { get; } = null;
            public Common.Patterns.TimeZoneInfo.IZone TimeZone { get; } = null;
            public DateTimeOffset EventTime { get; } = DateTimeOffset.UtcNow;
            public DateTime EventTimeLocal { get; } = DateTime.UtcNow;
            public DateTimeOffset? EventTimeBegin { get; } = null;
            public DateTimeOffset? EventTimeEnd { get; } = null;
            public TimeSpan? Duration { get; } = null;

            public DSEInfo.InstanceTypes Product { get; } = DSEInfo.InstanceTypes.Cassandra;
            public IKeyspace Keyspace { get; } = null;
            public IDDLStmt TableViewIndex { get; } = null;

            public bool Equals(Guid other)
            {
                throw new NotImplementedException();
            }

            public bool Equals(IEvent item)
            {
                if (ReferenceEquals(this, item)) return true;
                
                if(item is CommonPartitionKey itemEvent                    
                        && this.DataCenter == item.DataCenter)
                {                    
                    return this.Compare(itemEvent.PartitionKey);
                }

                return false;
            }

            public int CompareTo(IEvent other)
            {
                throw new NotImplementedException();
            }

            #endregion

            #region IAggregatedStats
            [JsonProperty(PropertyName = "Data")]
#pragma warning disable IDE1006 // Naming Styles
            private Dictionary<string, object> datamemberData
#pragma warning restore IDE1006 // Naming Styles
            {
                get { return this._data.UnSafe; }
                set { this._data = new CTS.Dictionary<string, object>(value); }
            }

            private CTS.Dictionary<string, object> _data = new CTS.Dictionary<string, object>();

            [JsonIgnore]
            public IDictionary<string, object> Data { get { return this._data; } }

            public IAggregatedStats AssociateItem(string key, object value)
            {
                this._data.Add(key, value);
                return this;
            }

            public IAggregatedStats UpdateAssociateItem(string key, object value)
            {
                this._data.AddOrUpdate(key, value, (k,v) => value);
                return this;
            }

            [JsonProperty(PropertyName = "ReconciliationRefs")]
#pragma warning disable IDE1006 // Naming Styles
            private List<int> datamemberReconciliationRefs
#pragma warning restore IDE1006 // Naming Styles
            {
                get { return this._reconciliationRefs.UnSafe; }
                set { this._reconciliationRefs = new CTS.List<int>(value); }
            }

            private CTS.List<int> _reconciliationRefs = new CTS.List<int>();

            [JsonIgnore]
            public IEnumerable<int> ReconciliationRefs { get { return this._reconciliationRefs; } }

            public IAggregatedStats AssociateItem(int reference)
            {
                if (!this._reconciliationRefs.Contains(reference))
                {
                    this._reconciliationRefs.Add(reference);
                }
                return this;
            }

            #endregion

            [JsonIgnore]
            public IEnumerable<ICQLColumn> PartitionKey
            {
                get
                {                   
                    if(this._data.TryGetValue(PKPropKey, out object propValue))
                    {
                        return (IEnumerable<ICQLColumn>)propValue;
                    }

                    return null;
                }
            }

            [JsonIgnore]
            //Can be Materialized Views
            public IEnumerable<ICQLTable> Tables
            {
                get
                {                   
                    if (this._data.TryGetValue(TablesPropKey, out object propValue))
                    {
                        return (IEnumerable<ICQLTable>)propValue;
                    }

                    return null;
                }
            }

            public void AssociateItem(ICQLTable assocTable)
            {                
                lock(this._data.UnSafe)
                {                   
                    if (this._data.UnSafe.TryGetValue(TablesPropKey, out object propValue))
                    {
                        ((IList<ICQLTable>)propValue).Add(assocTable);
                    }
                    else
                    {
                        this._data.UnSafe.Add(TablesPropKey, new List<ICQLTable>() { assocTable });
                    }
                }             
            }

            public static bool Compare(IEnumerable<ICQLColumn> compareToA, IEnumerable<ICQLColumn> compareToB)
            {
                if (compareToA == null || compareToB == null) return false;

                var pk = compareToA.GetEnumerator();
                var pkCompareTo = compareToB.GetEnumerator();                
                bool moveNext = false;
                bool returnValue = false;

                for(; ; )
                {
                    if((moveNext = pk.MoveNext()) == pkCompareTo.MoveNext())
                    {
                        if (moveNext) //Still enumerating through collection...
                        {
                            if (pk.Current.Equals(pkCompareTo.Current))
                            {
                                continue; //Matched so continue
                            }
                            else
                            {
                                returnValue = false; //Did not match so return false.
                            }
                        }
                        else //Both at end-of-collection, so all matched return true...
                        {
                            returnValue = true;
                        }
                        break;
                    }

                    //one collection at end-of-collection while other was not (i.e., collection counts were not the same)
                    returnValue = false;
                    break;
                }
                
                return returnValue;
            }

            public bool Compare(IEnumerable<ICQLColumn> compareTo)
            {
                return Compare(this.PartitionKey, compareTo);
            }

            public bool Equals(IEnumerable<ICQLColumn> item)
            {
                return item != null && this.Compare(item);
            }

            public bool Equals(ICQLTable item)
            {
                return item != null
                        && !item.Keyspace.LocalStrategy
                        && (item.Keyspace.EverywhereStrategy
                            || (this.DataCenter == null ? this.Cluster == item.Cluster
                                                        : item.Keyspace.Replications.Any(r => r.DataCenter == this.DataCenter)))
                        && this.Compare(item.PrimaryKeys);
            }
            
            #region Override

            public override string ToString()
            {
                return string.Format("AggregatedStats<{0}{1}{2}{3}{4} {5}, {6}, {7}, {8}, {9}{10:###,###,##0}>",
                                        this.DataCenter == null ? string.Empty : string.Format("{0}, ", this.DataCenter.Name),
                                        this.Node == null ? string.Empty : string.Format("{0}, ", this.Node),
                                        this.Keyspace == null ? string.Empty : string.Format("{0}, ", this.Keyspace.Name),
                                        this.TableViewIndex == null ? string.Empty : string.Format("{0}, ", this.TableViewIndex.Name),
                                        this.Node != null || this.DataCenter != null ? string.Empty : string.Format("{0}, ", this.Path.Name),
                                        this.EventTimeLocal,
                                        this.Source,
                                        this.Type,
                                        this.Class,
                                        this.SubClass == null ? string.Empty : string.Format("{0}, ", this.SubClass),
                                        this._data.Count);
            }

            #endregion
        }

        #endregion

        public sealed class PrimaryKeysComparer : IEqualityComparer<IEnumerable<ICQLColumn>>
        {
            public bool Equals(IEnumerable<ICQLColumn> x, IEnumerable<ICQLColumn> y)
            {
                return CommonPartitionKey.Compare(x, y);
            }

            public int GetHashCode(IEnumerable<ICQLColumn> obj)
            {
                return string.Join(",", obj.Select(c => c.Name + " " + c.CQLType.Name)).GetHashCode();
            }
        }

        public override IEnumerable<IAggregatedStats> ComputeStats()
        {
            var tablesWSamePK = this.Cluster.Keyspaces.Where(k => !k.LocalStrategy && !this.IgnoreKeySpaces.Any(t => t == k.Name))
                                        .SelectMany(k => k.DDLs.OfType<ICQLTable>())                                        
                                        .GroupBy(t => t.PrimaryKeys, new PrimaryKeysComparer())
                                        .Where(g => g.Count() > 1);
            var newStats = new List<CommonPartitionKey>();
            CommonPartitionKey commonPK;

            foreach (var pkGrp in tablesWSamePK)
            {
                commonPK = new CommonPartitionKey(this.Cluster, pkGrp.Key);
                newStats.Add(commonPK);
                this.Cluster.AssociateItem(commonPK);

                foreach(var table in pkGrp)
                {
                    commonPK.AssociateItem(table);
                }
            }

            return newStats;
        }
    }
}
