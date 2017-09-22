using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using Common.Patterns.TimeZoneInfo;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticLibrary
{
    public interface IAggregatedStats : IEvent
    {
        /// <summary>
        /// A collection of reference ids used to make up this aggregation set.
        /// </summary>
        IEnumerable<int> ReconciliationRefs { get;}
        /// <summary>
        /// Returns a dictionary where the key is the property name and the value is the aggregated data value.
        /// </summary>
        IDictionary<string, object> Data { get; }

        IAggregatedStats AssociateItem(string key, object value);
        IAggregatedStats AssociateItem(int reference);
    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class AggregatedStats : IAggregatedStats
    {

        #region Constructors
        private AggregatedStats(IPath filePath,
                                SourceTypes source,
                                EventTypes eventType,
                                EventClasses eventClass,
                                DSEInfo.InstanceTypes product,
                                IZone timeZone,
                                string subClass,
                                DateTimeRange aggregatedTimePeriod)
        {
            this.Id = new Guid();

            this.Path = filePath;
            this.Source = source;
            this.Type = eventType;
            this.Class = eventClass;
            this.Product = product;
            this.SubClass = subClass;
            this.TimeZone = timeZone;

            if (aggregatedTimePeriod != null)
            {
                this.EventTimeLocal = aggregatedTimePeriod.Max;
                this.Duration = aggregatedTimePeriod.Max - aggregatedTimePeriod.Min;

                if (this.TimeZone == null)
                {
                    this.EventTime = this.EventTimeLocal;
                    this.EventTimeBegin = aggregatedTimePeriod.Min;
                    this.EventTimeEnd = aggregatedTimePeriod.Max;
                }
                else
                {
                    this.EventTime = this.TimeZone.ConvertToUTCOffset(this.EventTimeLocal).Value;
                    this.EventTimeBegin = this.TimeZone.ConvertToUTCOffset(aggregatedTimePeriod.Min);
                    this.EventTimeEnd = this.TimeZone.ConvertToUTCOffset(aggregatedTimePeriod.Max);
                }
            }
        }

        public AggregatedStats(IPath filePath,
                                INode node,
                                SourceTypes source,
                                EventTypes eventType,
                                EventClasses eventClass,
                                IDDL DDLItem = null,
                                DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                string subClass = null,
                                DateTimeRange aggregatedTimePeriod = null,
                                bool assocateToNode = true,
                                bool assocateToDDLItem = false)
            : this(filePath,
                    source,
                    eventType,
                    eventClass,
                    product,
                    node.Machine.TimeZone,
                    subClass,
                    aggregatedTimePeriod)
        {
            this.Node = node;
            this.DataCenter = this.Node.DataCenter;
            this.Cluster = this.DataCenter.Cluster;

            if(DDLItem != null)
            {
                if(DDLItem is IKeyspace)
                {
                    this.Keyspace = (IKeyspace)DDLItem;

                    if(assocateToDDLItem)
                    {
                        ((IKeyspace)DDLItem).AssociateItem(this);
                    }
                }
                else if(DDLItem is IDDLStmt)
                {
                    this.Keyspace = ((IDDLStmt)DDLItem).Keyspace;
                    this.TableViewIndex = (IDDLStmt)DDLItem;

                    if (assocateToDDLItem)
                    {
                        ((IDDLStmt)DDLItem).AssociateItem(this);
                    }
                }
            }


            if(assocateToNode)
            {
                this.Node.AssociateItem(this);
            }
        }

        public AggregatedStats(IPath filePath,
                                IDataCenter dataCenter,
                                SourceTypes source,
                                EventTypes eventType,
                                EventClasses eventClass,
                                DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                string subClass = null,
                                DateTimeRange aggregatedTimePeriod = null,
                                bool assocateToDC = true)
            : this(filePath,
                    source,
                    eventType,
                    eventClass,
                    product,
                    dataCenter.TimeZone,
                    subClass,
                    aggregatedTimePeriod)
        {
            this.DataCenter = dataCenter;
            this.Cluster = this.DataCenter.Cluster;

            if (assocateToDC)
            {
                this.DataCenter.AssociateItem(this);
            }
        }

        public AggregatedStats(IPath filePath,
                                Cluster cluster,
                                SourceTypes source,
                                EventTypes eventType,
                                EventClasses eventClass,
                                DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                string subClass = null,
                                DateTimeRange aggregatedTimePeriod = null,
                                bool assocateToCluster = true)
            : this(filePath,
                    source,
                    eventType,
                    eventClass,
                    product,
                    null,
                    subClass,
                    aggregatedTimePeriod)
        {
            this.Cluster = cluster;

            if (assocateToCluster)
            {
                this.Cluster.AssociateItem(this);
            }
        }

        public AggregatedStats(IPath filePath,
                                KeySpace keyspace,
                                SourceTypes source,
                                EventTypes eventType,
                                EventClasses eventClass,
                                DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                string subClass = null,
                                DateTimeRange aggregatedTimePeriod = null,
                                bool assocateToKeyspace = true)
            : this(filePath,
                    source,
                    eventType,
                    eventClass,
                    product,
                    keyspace.Node?.Machine.TimeZone ?? keyspace.DataCenter?.TimeZone,
                    subClass,
                    aggregatedTimePeriod)
        {
            this.Keyspace = keyspace;
            this.DataCenter = keyspace.DataCenter;
            this.Cluster = keyspace.Cluster;
            this.Node = keyspace.Node;

            if (assocateToKeyspace)
            {
                this.Keyspace.AssociateItem(this);
            }
        }

        public AggregatedStats(IPath filePath,
                                IDDLStmt ddlItem,
                                SourceTypes source,
                                EventTypes eventType,
                                EventClasses eventClass,
                                DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                string subClass = null,
                                DateTimeRange aggregatedTimePeriod = null,
                                bool assocateToDDL = true)
            : this(filePath,
                    source,
                    eventType,
                    eventClass,
                    product,
                    ddlItem.Node?.Machine.TimeZone ?? ddlItem.DataCenter?.TimeZone,
                    subClass,
                    aggregatedTimePeriod)
        {
            this.TableViewIndex = ddlItem;
            this.Keyspace = this.TableViewIndex.Keyspace;
            this.DataCenter = this.TableViewIndex.DataCenter;
            this.Cluster = this.TableViewIndex.Cluster;
            this.Node = this.TableViewIndex.Node;

            if (assocateToDDL)
            {
                this.TableViewIndex.AssociateItem(this);
            }
        }
        #endregion

        #region IParsed Members

        public SourceTypes Source { get; }
        public IPath Path { get; }
        public Cluster Cluster { get; }
        public IDataCenter DataCenter { get; }
        public INode Node { get; }
        public int Items { get; } = 1;
        public uint LineNbr { get; } = 0;

        #endregion

        #region IEvent

        public EventTypes Type { get; }
        public EventClasses Class { get; }
        /// <summary>
        /// e.g., Hint, Tombstone, etc.
        /// </summary>
        public string SubClass { get; }

        /// <summary>
        /// If not defined one is generated. For repair session this should by the guid from the log
        /// For Aggregate Data this is for the complete set/collection (e.g., CFStats for a node)
        /// </summary>
        public Guid Id { get; }
        /// <summary>
        /// Events associated with this event so that nesting
        /// </summary>
        [JsonIgnore]
        public IEnumerable<IEvent> ParentEvents
        {
            get
            {
                return null;
            }

        }
        public IZone TimeZone { get; }
        /// <summary>
        /// Time event Occurred (e.g., This would be the logged time)
        /// </summary>
        public DateTimeOffset EventTime { get; }
        public DateTime EventTimeLocal { get; }
        /// <summary>
        /// For a timespan event, this would be the beginning timestamp.
        /// </summary>
        public DateTimeOffset? EventTimeBegin { get; }
        /// <summary>
        /// For a timespan event, this would be the ending timestamp.
        /// </summary>
        public DateTimeOffset? EventTimeEnd { get; }
        public TimeSpan? Duration { get; }

        public DSEInfo.InstanceTypes Product { get; }
        public IKeyspace Keyspace { get; }
        public IDDLStmt TableViewIndex { get; }

        public bool Equals(Guid other)
        {
            return this.Id == other;
        }

        public bool Equals(IEvent item)
        {
            if (ReferenceEquals(this, item)) return true;
            if (item.Id == this.Id) return true;

            return this.Source == item.Source
                    && this.Product == item.Product
                    && this.Type == item.Type
                    && this.Class == item.Class
                    && this.SubClass == item.SubClass
                    && this.Cluster == item.Cluster
                    && this.DataCenter == item.DataCenter
                    && this.Node == item.Node
                    && this.Keyspace == item.Keyspace
                    && this.TableViewIndex == item.TableViewIndex
                    && this.EventTime == item.EventTime
                    && this.Duration == item.Duration;
        }

        public int CompareTo(IEvent other)
        {
            if(this.EventTimeBegin.HasValue && other.EventTimeBegin.HasValue)
            {
                if(this.EventTimeBegin.Value == other.EventTimeBegin.Value)
                {
                    if (this.EventTimeEnd.HasValue && other.EventTimeEnd.HasValue)
                        return this.EventTimeEnd.Value.CompareTo(other.EventTimeEnd.Value);
                    return 0;
                }
                return this.EventTimeBegin.Value.CompareTo(other.EventTimeBegin.Value);
            }

            return this.EventTimeLocal.CompareTo(other.EventTimeLocal);
        }

        #endregion

        [JsonProperty(PropertyName = "Data")]
        private Dictionary<string, object> datamemberData
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

        [JsonProperty(PropertyName = "ReconciliationRefs")]
        private List<int> datamemberReconciliationRefs
        {
            get { return this._reconciliationRefs.UnSafe; }
            set { this._reconciliationRefs = new CTS.List<int>(value); }
        }

        private CTS.List<int> _reconciliationRefs = new CTS.List<int>();

        [JsonIgnore]
        public IEnumerable<int> ReconciliationRefs { get { return this._reconciliationRefs; } }

        public IAggregatedStats AssociateItem(int reference)
        {
            if(!this._reconciliationRefs.Contains(reference))
            {
                this._reconciliationRefs.Add(reference);
            }
            return this;
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
}
