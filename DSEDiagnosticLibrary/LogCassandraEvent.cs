using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using Common.Patterns.TimeZoneInfo;
using Common.Patterns.Collections;
using IMMLogValue = Common.Patterns.Collections.MemoryMapped.IMMValue<DSEDiagnosticLibrary.ILogEvent>;

namespace DSEDiagnosticLibrary
{

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class LogCassandraEvent : ILogEvent
    {
        static readonly IReadOnlyDictionary<string, object> EmptyLogProperties = new System.Collections.ObjectModel.ReadOnlyDictionary<string, object>(new Dictionary<string, object>(0));
        static readonly IEnumerable<string> EmptySSTables = Enumerable.Empty<string>();
        static readonly IEnumerable<IDDLStmt> EmptyDDLItems = Enumerable.Empty<IDDLStmt>();
        static readonly IEnumerable<INode> EmptyAssocatedNodes = Enumerable.Empty<INode>();
        static readonly IEnumerable<IMMLogValue> EmptyParents = Enumerable.Empty<IMMLogValue>();
        static readonly IEnumerable<TokenRangeInfo> EmptyTokenRanges = Enumerable.Empty<TokenRangeInfo>();

        static Guid GenerateLogId(string logId)
        {
            if(!string.IsNullOrEmpty(logId))
                try
                {
                    return new Guid(logId);
                }
                catch
                {}

            return Guid.NewGuid();
        }

        public LogCassandraEvent(IFilePath logFile,
                                    INode node,
                                    uint lineNbr,
                                    EventClasses logEventClass,
                                    DateTime logLocalTime,
                                    DateTimeOffset logLocalTimewOffset,
                                    string logMessage,
                                    EventTypes eventType,
                                    string logId = null,
                                    string subClass = null,
                                    IEnumerable<ILogEvent> parentEvents = null,
                                    IZone timeZone = null,
                                    IKeyspace keyspace = null,
                                    IDDLStmt tableviewindexItem = null,
                                    TimeSpan? duration = null,
                                    IReadOnlyDictionary<string, object> logProperties = null,
                                    IEnumerable<string> ssTables = null,
                                    IEnumerable<IDDLStmt> ddlItems = null,
                                    IEnumerable<INode> assocatedNodes = null,
                                    IEnumerable<TokenRangeInfo> tokenRanges = null,
                                    string sessionTieOutId = null,
                                    DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                    bool assocateToNode = false,
                                    string analyticsGroup = null,
                                    NodeStateChange.DetectedStates? nodeTransitionState = null,
                                    bool tempEvent = false)
        {
            if (logFile == null) throw new ArgumentNullException("logFile cannot be null");
            if (node == null) throw new ArgumentNullException("node cannot be null");
            if (string.IsNullOrEmpty(logMessage)) throw new ArgumentNullException("logMessage cannot be null");

            this.Source = SourceTypes.CassandraLog;
            this.Id = GenerateLogId(logId);
            this.Path = logFile;
            this.Node = node;
#if DEBUG
            this.LogMessage = logMessage;
#else
            this.LogMessage = logMessage?.GetHashCode() ?? 0;
#endif
            this.LineNbr = lineNbr;
            this.Type = eventType;
            this.Class = logEventClass;
            this.SubClass = subClass.InternString();
            this.ParentEvents = parentEvents?.Select(p => new Common.Patterns.Collections.MemoryMapped.MMValue<ILogEvent>(p)) ?? EmptyParents;
            this.TimeZone = timeZone;
            this.EventTimeLocal = logLocalTime;
            this.EventTime = logLocalTimewOffset;
            this.Duration = duration;
            this._keyspace = keyspace;
            this.TableViewIndex = tableviewindexItem;
            this.Product = product;
            this.LogProperties = logProperties ?? EmptyLogProperties;
            this.AnalyticsGroup = analyticsGroup.InternString();
            this.NodeTransitionState = nodeTransitionState.HasValue
                                        ? (nodeTransitionState.Value == NodeStateChange.DetectedStates.None
                                                ? null
                                                : nodeTransitionState)
                                        : null;
            this.SSTables = ssTables ?? EmptySSTables;
            this.DDLItems = ddlItems ?? EmptyDDLItems;
            this.AssociatedNodes = assocatedNodes ?? EmptyAssocatedNodes;
            this.SessionTieOutId = sessionTieOutId;
            this.TokenRanges = tokenRanges ?? EmptyTokenRanges;
            this.Items = this.LogProperties.Count()
                            + this.SSTables.Count()
                            + this.DDLItems.Count()
                            + this.AssociatedNodes.Count()
                            + this.TokenRanges.Count()
                            + 1;

            if (this.TimeZone != null && this.EventTime.Offset == TimeSpan.Zero)
            {
                this.EventTime = Common.TimeZones.ConvertToOffset(this.EventTimeLocal, this.TimeZone);
            }

            if((this.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin)
            {
                this.EventTimeBegin = this.EventTime;
            }

            this.TempEvent = tempEvent;
            if(assocateToNode && !tempEvent)
            {
                this.Node.AssociateItem(this);                
            }
        }

        public LogCassandraEvent(IFilePath logFile,
                                   INode node,
                                   uint lineNbr,
                                   EventClasses logEventClass,
                                   DateTime logLocalTime,
                                   DateTimeOffset logLocalTimewOffset,
                                   UnitOfMeasure uomDuration,
                                   string logMessage,
                                   EventTypes eventType,
                                   string logId = null,
                                   string subClass = null,
                                   IEnumerable<ILogEvent> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<TokenRangeInfo> tokenRanges = null,
                                   string sessionTieOutId = null,
                                   DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                   bool assocateToNode = false,
                                   string analyticsGroup = null,
                                   NodeStateChange.DetectedStates? nodeTransitionState = null,
                                   bool tempEvent = false)
            :  this(logFile,
                        node,
                        lineNbr,
                        logEventClass,
                        logLocalTime,
                        logLocalTimewOffset,
                        logMessage,
                        eventType,
                        logId,
                        subClass,
                        parentEvents,
                        timeZone,
                        keyspace,
                        tableviewindexItem,
                        null,
                        logProperties,
                        ssTables,
                        ddlItems,
                        assocatedNodes,
                        tokenRanges,
                        sessionTieOutId,
                        product,
                        assocateToNode,
                        analyticsGroup,
                        nodeTransitionState,
                        tempEvent)
        {
            this.Duration = (TimeSpan)uomDuration;
            this.EventTimeBegin = this.EventTime - this.Duration;
            this.EventTimeEnd = this.EventTime;
        }

        public LogCassandraEvent(IFilePath logFile,
                                   INode node,
                                   EventClasses logEventClass,
                                   DateTime logLocalTime,
                                   DateTimeOffset logStartTimewOffset,
                                   double durationMS,
                                   EventTypes eventType,
                                   string logMessage,
                                   DateTimeOffset? logEndTimewOffset = null,
                                   uint lineNbr = 0,                                   
                                   string logId = null,
                                   string subClass = null,
                                   IEnumerable<ILogEvent> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<TokenRangeInfo> tokenRanges = null,
                                   string sessionTieOutId = null,
                                   DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                   bool assocateToNode = false,
                                   string analyticsGroup = null,
                                   NodeStateChange.DetectedStates? nodeTransitionState = null,
                                   bool tempEvent = false)
            :  this(logFile,
                        node,
                        lineNbr,
                        logEventClass,
                        logLocalTime,
                        logStartTimewOffset,
                        logMessage,
                        eventType,
                        logId,
                        subClass,
                        parentEvents,
                        timeZone,
                        keyspace,
                        tableviewindexItem,
                        null,
                        logProperties,
                        ssTables,
                        ddlItems,
                        assocatedNodes,
                        tokenRanges,
                        sessionTieOutId,
                        product,
                        assocateToNode,
                        analyticsGroup,
                        nodeTransitionState,
                        tempEvent)
        {
            this.Duration = TimeSpan.FromMilliseconds(durationMS);
            if (logEndTimewOffset.HasValue)
            {
                this.EventTimeBegin = logStartTimewOffset;
                this.EventTimeEnd = logEndTimewOffset.Value;
            }
        }

        public LogCassandraEvent(IFilePath logFile,
                                   INode node,
                                   uint lineNbr,
                                   EventClasses logEventClass,
                                   DateTime logLocalTime,
                                   DateTimeOffset logLocalTimewOffset,
                                   DateTimeOffset logBeginTime,
                                   string logMessage,
                                   EventTypes eventType,
                                   string logId = null,
                                   string subClass = null,
                                   IEnumerable<ILogEvent> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<TokenRangeInfo> tokenRanges = null,
                                   string sessionTieOutId = null,
                                   DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                   bool assocateToNode = false,
                                   string analyticsGroup = null,
                                   NodeStateChange.DetectedStates? nodeTransitionState = null,
                                   bool tempEvent = false)
            : this(logFile,
                        node,
                        lineNbr,
                        logEventClass,
                        logLocalTime,
                        logLocalTimewOffset,
                        logMessage,
                        eventType,
                        logId,
                        subClass,
                        parentEvents,
                        timeZone,
                        keyspace,
                        tableviewindexItem,
                        null,
                        logProperties,
                        ssTables,
                        ddlItems,
                        assocatedNodes,
                        tokenRanges,
                        sessionTieOutId,
                        product,
                        assocateToNode,
                        analyticsGroup,
                        nodeTransitionState,
                        tempEvent)
        {
            this.EventTimeBegin = logBeginTime;
            this.EventTimeEnd = this.EventTime;
            this.Duration = this.EventTimeEnd.Value - this.EventTimeBegin.Value;
        }

        public LogCassandraEvent(IFilePath logFile,
                                   INode node,
                                   uint lineNbr,
                                   EventClasses logEventClass,
                                   DateTime logLocalTime,
                                   DateTimeOffset logLocalTimewOffset,
                                   DateTime logLocalBeginTime,
                                   string logMessage,
                                   EventTypes eventType,
                                   string logId = null,
                                   string subClass = null,
                                   IEnumerable<ILogEvent> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<TokenRangeInfo> tokenRanges = null,
                                   string sessionTieOutId = null,
                                   DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                   bool assocateToNode = false,
                                   string analyticsGroup = null,
                                   NodeStateChange.DetectedStates? nodeTransitionState = null,
                                   bool tempEvent = false)
            : this(logFile,
                        node,
                        lineNbr,
                        logEventClass,
                        logLocalTime,
                        logLocalTimewOffset,
                        logMessage,
                        eventType,
                        logId,
                        subClass,
                        parentEvents,
                        timeZone,
                        keyspace,
                        tableviewindexItem,
                        null,
                        logProperties,
                        ssTables,
                        ddlItems,
                        assocatedNodes,
                        tokenRanges,
                        sessionTieOutId,
                        product,
                        assocateToNode,
                        analyticsGroup,
                        nodeTransitionState,
                        tempEvent)
        {
            this.EventTimeBegin = new DateTimeOffset(logLocalBeginTime, this.EventTime.Offset);
            this.EventTimeEnd = this.EventTime;
            this.Duration = this.EventTimeEnd.Value - this.EventTimeBegin.Value;
        }

        #region IParsed

        public SourceTypes Source { get; }
        [JsonConverter(typeof(IPathJsonConverter))]
        public IPath Path { get; }
        [JsonIgnore]
        public Cluster Cluster { get { return this.Node?.Cluster; } }
        [JsonIgnore]
        public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
        public INode Node { get; }
        public int Items { get; }
        public uint LineNbr { get; }

        #endregion

        #region IEvent

        public EventTypes Type { get; }
        public EventClasses Class { get; private set; }
        /// <summary>
        /// e.g., Hint, Tombstone, etc.
        /// </summary>
        public string SubClass { get; }

        /// <summary>
        /// If not defined one is generated. For session this should by the same guid for the complete session.
        /// </summary>
        public Guid Id { get; private set; }

        [JsonProperty(PropertyName="TimeZone")]
        private string datamemberTimeZone
        {
            get { return this.TimeZone?.Name; }
            set { this.TimeZone = StringHelpers.FindTimeZone(value); }
        }
        [JsonIgnore]
        public IZone TimeZone { get; private set; }
        /// <summary>
        /// Time event Occurred with time zone offset (e.g., This would be the logged time)
        /// </summary>
        public DateTimeOffset EventTime { get; }
        public DateTime EventTimeLocal { get; }
        public DateTimeOffset? EventTimeBegin { get; private set; }
        public DateTimeOffset? EventTimeEnd { get; private set; }
        public TimeSpan? Duration { get; private set; }
        [JsonProperty(PropertyName="Keyspace")]
        private IKeyspace _keyspace = null;
        [JsonIgnore]
        public IKeyspace Keyspace { get { return this._keyspace ?? this.TableViewIndex?.Keyspace; } }
        public IDDLStmt TableViewIndex { get; private set; }
        public DSEInfo.InstanceTypes Product { get; }

        public int CompareTo(IEvent other)
        {
            if (this.EventTimeBegin.HasValue && other.EventTimeBegin.HasValue)
            {
                if (this.EventTimeBegin.Value == other.EventTimeBegin.Value)
                {
                    if (this.EventTimeEnd.HasValue && other.EventTimeEnd.HasValue)
                        return this.EventTimeEnd.Value.CompareTo(other.EventTimeEnd.Value);
                    return 0;
                }
                return this.EventTimeBegin.Value.CompareTo(other.EventTimeBegin.Value);
            }

            return this.EventTimeLocal.CompareTo(other.EventTimeLocal);
        }

        IEnumerable<IEvent> IEvent.ParentEvents { get; }

        #endregion

        #region IMemoryMapperElement

        public enum ElementCreationTypes
        {
            //
            // Summary:
            //     All fields should be read and populated from the memory map
            FullView = 0,
            //
            // Summary:
            //     Do not populate the instance during instantiation instead it is populated during
            //     the Common.Patterns.Collections.MemoryMapped.List.EnumValue.GetValue action body.
            DoNotPopulate = 1,
            //
            // Summary:
            //     Only fields required to satisfy the IComparable.CompareTo method. Note that this
            //     type is passed to the constructor upon the use of the memory map list's Sort
            //     methods.
            ComparableView = 2,
            //
            // Summary:
            //     Only fields required for grouping
            GroupingView = 4,
            //
            // Summary:
            //     Only fields required to identify an instance used for searches, finds, where,
            //     etc.
            SearchView = 8,
            SessionTieOutIdOnly = 9,
            EventTypeOnly = 10,
            AggregationPeriodOnly = 11,
            AggregationPeriodOnlyWProps = 12,
            AggregationPeriodOnlyWPropsTZ = 13,
            DCNodeKSDDLTypeClassOnly = 14
        }
        
        public LogCassandraEvent(MemoryMapper.ReadElement readView, MemoryMapperElementCreationTypes instanceType)
        {
            this._hashcode = readView.GetIntValue();

            if (instanceType == MemoryMapperElementCreationTypes.DoNotPopulate) return;

            ElementCreationTypes creationType = (ElementCreationTypes)instanceType;

            if(creationType == ElementCreationTypes.SessionTieOutIdOnly)
            {                
                readView.SkipInt();
                readView.SkipStruct();
                readView.SkipInt();
                readView.SkipInt();
                readView.SkipString();
                readView.SkipInt();
                readView.SkipInt();
                readView.SkipLong();
                this.SessionTieOutId = readView.GetStringValue();
                return;
            }

            if (creationType == ElementCreationTypes.EventTypeOnly)
            {                
                readView.SkipInt();
                this.EventTime = readView.GetStructValue<DateTimeOffset>();
                readView.SkipInt();
                readView.SkipInt();
                readView.SkipString();
                readView.SkipInt(); //Source
                this.Type = (EventTypes)readView.GetIntValue();
                this.Class = (EventClasses)readView.GetLongValue();
                return;
            }

            if (creationType == ElementCreationTypes.DCNodeKSDDLTypeClassOnly)
            {
                int hashCode1;
               
                hashCode1 = readView.GetIntValue();
                this.Node = hashCode1 == 0 ? null : Cluster.TryGetNode(hashCode1);
                this.EventTime = readView.GetStructValue<DateTimeOffset>();
                hashCode1 = readView.GetIntValue();
                this._keyspace = hashCode1 == 0 ? null : Cluster.TryGetKeySpace(hashCode1);
                hashCode1 = readView.GetIntValue();
                this.TableViewIndex = hashCode1 == 0 ? null : Cluster.TryGetDDLStmt(hashCode1);
                readView.SkipString(); //id
                this.Source = (SourceTypes)readView.GetIntValue();
                this.Type = (EventTypes)readView.GetIntValue();
                this.Class = (EventClasses)readView.GetLongValue();
                return;
            }

            int hashCode;
            string strValue;

            hashCode = readView.GetIntValue();
            this.Node = hashCode == 0 ? null : Cluster.TryGetNode(hashCode);
            this.EventTime = readView.GetStructValue<DateTimeOffset>();
            hashCode = readView.GetIntValue();
            this._keyspace = hashCode == 0 ? null : Cluster.TryGetKeySpace(hashCode);
            hashCode = readView.GetIntValue();
            this.TableViewIndex = hashCode == 0 ? null : Cluster.TryGetDDLStmt(hashCode);
            strValue = readView.GetStringValue();
            if(!string.IsNullOrEmpty(strValue))
            {
                this.Id = new Guid(strValue);
            }

            if (creationType == ElementCreationTypes.FullView
                    || creationType == ElementCreationTypes.SearchView)
            {
                this.Source = (SourceTypes)readView.GetIntValue();
                this.Type = (EventTypes)readView.GetIntValue();
                this.Class = (EventClasses)readView.GetLongValue();
                this.SessionTieOutId = readView.GetStringValue();
                this.ParentEvents = readView.GetEnumerableValue<int>()
                                                .Select(e => new Common.Patterns.Collections.MemoryMapped.List<LogCassandraEvent, ILogEvent>.MMValue(readView.MemoryMapper,
                                                                                                                                                        readView.MemoryMapper.Elements
                                                                                                                                                                                .Find(f => f.ElementHashCode == e)));
            }

            if (creationType == ElementCreationTypes.AggregationPeriodOnly
                    || creationType == ElementCreationTypes.AggregationPeriodOnlyWProps
                    || creationType == ElementCreationTypes.AggregationPeriodOnlyWPropsTZ)
            {
                this.Source = (SourceTypes)readView.GetIntValue();
                this.Type = (EventTypes)readView.GetIntValue();
                this.Class = (EventClasses)readView.GetLongValue();
                readView.SkipString();
                readView.SkipStructEnumerable();
                // IParsed Members
                readView.SkipString();
                readView.SkipInt();
                readView.SkipUInt();
                //IEvents members
                this.SubClass = readView.GetStringValue().InternString();
                if(creationType == ElementCreationTypes.AggregationPeriodOnlyWPropsTZ)
                {
                    strValue = readView.GetStringValue();
                    this.TimeZone = string.IsNullOrEmpty(strValue) ? null : Common.TimeZones.Find(strValue);
                }
                else                
                    readView.SkipString();
                this.EventTimeLocal = readView.GetStructValue<DateTime>();
                this.EventTimeBegin = readView.GetNullableValue<DateTimeOffset>();
                this.EventTimeEnd = readView.GetNullableValue<DateTimeOffset>();
                this.Duration = readView.GetNullableValue<TimeSpan>();
                this.Product = (DSEInfo.InstanceTypes)readView.GetIntValue();
                //Log Members
                this.AnalyticsGroup = readView.GetStringValue().InternString();
                if (creationType == ElementCreationTypes.AggregationPeriodOnlyWProps || creationType == ElementCreationTypes.AggregationPeriodOnlyWPropsTZ)
                    this.LogProperties = readView.ReadMMElement(this.Cluster, this.DataCenter, this.Node, this.Keyspace);
                else
                    readView.SkipMMElementKVPStrObj();
#if DEBUG
                readView.SkipString();
#else
                readView.SkipInt();
#endif
                if (creationType == ElementCreationTypes.AggregationPeriodOnlyWProps)
                {
                    this.SSTables = readView.GetEnumerableString();
                    this.DDLItems = readView.GetEnumerableValue<int>().Select(h => Cluster.TryGetDDLStmt(h));
                    this.AssociatedNodes = readView.GetEnumerableValue<int>().Select(h => Cluster.TryGetNode(h));
                    this.TokenRanges = readView.GetMMTokenRangeEnum();
                }
                else
                {
                    readView.SkipStringEnumerable();
                    readView.SkipStructEnumerable();
                    readView.SkipStructEnumerable();
                    readView.SkipMMTokenRangeEnum();
                }               
                this.Exception = readView.GetStringValue();
                this.ExceptionPath = readView.GetEnumerableString();
            }
            else if (creationType == ElementCreationTypes.FullView)
            {
                //IParsed Members
                strValue = readView.GetStringValue();
                this.Path = string.IsNullOrEmpty(strValue) ? null : Common.File.BaseFile.Make(strValue);
                this.Items = readView.GetIntValue();
                this.LineNbr = readView.GetUIntValue();
                //IEvents members
                this.SubClass = readView.GetStringValue().InternString();
                strValue = readView.GetStringValue();
                this.TimeZone = string.IsNullOrEmpty(strValue) ? null : Common.TimeZones.Find(strValue);
                this.EventTimeLocal = readView.GetStructValue<DateTime>();
                this.EventTimeBegin = readView.GetNullableValue<DateTimeOffset>();
                this.EventTimeEnd = readView.GetNullableValue<DateTimeOffset>();
                this.Duration = readView.GetNullableValue<TimeSpan>();
                this.Product = (DSEInfo.InstanceTypes) readView.GetIntValue();
                //Log Members
                this.AnalyticsGroup = readView.GetStringValue().InternString();
                this.LogProperties = readView.ReadMMElement(this.Cluster, this.DataCenter, this.Node, this.Keyspace);
#if DEBUG
                this.LogMessage = readView.GetStringValue();
#else
                this.LogMessage = readView.GetIntValue();
#endif
                this.SSTables = readView.GetEnumerableString();
                this.DDLItems = readView.GetEnumerableValue<int>().Select(h => Cluster.TryGetDDLStmt(h));
                this.AssociatedNodes = readView.GetEnumerableValue<int>().Select(h => Cluster.TryGetNode(h));
                this.TokenRanges = readView.GetMMTokenRangeEnum();
                this.Exception = readView.GetStringValue();
                this.ExceptionPath = readView.GetEnumerableString();
            }
        }

        public void GetObjectData(MemoryMapper.WriteElement writeView)
        {
            writeView.StoreValue(this.GetHashCode());
            writeView.StoreValue(this.Node?.GetHashCode() ?? 0);
            writeView.StoreValue(this.EventTime);
            writeView.StoreValue(this._keyspace?.GetHashCode() ?? 0);
            writeView.StoreValue(this.TableViewIndex?.GetHashCode() ?? 0);
            writeView.StoreValue(this.Id.ToString());

            writeView.StoreValue((int)this.Source);
            writeView.StoreValue((int)this.Type);
            writeView.StoreValue((long)this.Class);
            writeView.StoreValue(this.SessionTieOutId);

            writeView.StoreValue(this.ParentEvents.Select(p => p.GetHashCode()));

            //IParsed Members
            writeView.StoreValue((string) this.Path?.Path);
            writeView.StoreValue(this.Items);
            writeView.StoreValue(this.LineNbr);
            //IEvents members
            writeView.StoreValue(this.SubClass);
            writeView.StoreValue((string)this.TimeZone?.Name);
            writeView.StoreValue(this.EventTimeLocal);
            writeView.StoreValue(this.EventTimeBegin);
            writeView.StoreValue(this.EventTimeEnd);
            writeView.StoreValue(this.Duration);
            writeView.StoreValue((int)this.Product);
            //Log Members
            writeView.StoreValue(this.AnalyticsGroup);
            writeView.WriteMMElement(this.LogProperties);
            writeView.StoreValue(this.LogMessage);
            writeView.StoreValue(this.SSTables);
            writeView.StoreValue(this.DDLItems.Select(i => i.GetHashCode()));
            writeView.StoreValue(this.AssociatedNodes.Where(i => i != null).Select(i => i.GetHashCode()));
            writeView.WriteMMElement(this.TokenRanges);
            writeView.StoreValue(this.Exception);
            writeView.StoreValue(this.ExceptionPath);
        }


#endregion

#region ILogEvent

        public string SessionTieOutId { get; private set; }
        
        public IReadOnlyDictionary<string, object> LogProperties { get; private set; }
        public string AnalyticsGroup { get; }

        public NodeStateChange.DetectedStates? NodeTransitionState { get; }

#if DEBUG
        public string LogMessage { get; }
#else
        public int LogMessage { get; }
#endif
        public IEnumerable<string> SSTables { get; private set; }
        public IEnumerable<IDDLStmt> DDLItems { get; private set; }
        public IEnumerable<INode> AssociatedNodes { get; private set; }
        public IEnumerable<TokenRangeInfo> TokenRanges { get; private set; }

        /// <summary>
        /// Top level exception
        /// </summary>
        public string Exception { get; private set; }

        /// <summary>
        /// An collection that represents the exception path.
        /// 
        /// e.g., ExceptionTopLevel(Exception Description), ExceptionNextLevelB(Exception Description), ExceptionNextLevelC(Exception Description)  
        /// </summary>
        public IEnumerable<string> ExceptionPath { get; private set; }

        public IEnumerable<IMMLogValue> ParentEvents { get; private set; }

        /// <summary>
        /// This will return true if the two events are the same or similar based on source, class, type, keyspace, and DDL instance
        /// </summary>
        /// <param name="matchEvent"></param>
        /// <param name="mustMatchNode"></param>
        /// <returns></returns>
        public bool Match(IEvent matchEvent, bool mustMatchNode = true, bool mustMatchKeyspaceDDL = true)
        {
            if (matchEvent == null) return false;
            if (ReferenceEquals(this, matchEvent)) return true;

            if(this.Source == matchEvent.Source)
            {
                if (this.Id == matchEvent.Id) return true;

                if(!mustMatchNode || this.Node.Equals(matchEvent.Node))
                {
                    if((this.Class & ~EventClasses.StatusTypes) == (matchEvent.Class & ~EventClasses.StatusTypes))
                    {
                        if(this.Type == matchEvent.Type
                                || (this.Type & EventTypes.SessionElement) == EventTypes.SessionElement)
                        {
                            return !mustMatchKeyspaceDDL
                                        || (this.Keyspace?.FullName == matchEvent.Keyspace?.FullName
                                                && this.TableViewIndex?.Name == matchEvent.TableViewIndex?.Name);
                        }
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Returns true if the arguments match.
        /// </summary>
        /// <param name="possibleDDLName">
        /// Can be null, if so this argument is ignored
        /// If possibleKeyspaceName is given, the keyspace must also match the DDL statement.
        /// </param>
        /// <param name="possibleKeyspaceName">
        /// Can be null, if so this argument is ignored.
        /// </param>
        /// <param name="possibleNode">
        /// Can be null, if so this argument is ignored
        /// </param>
        /// <param name="checkDDLItems"></param>
        /// <returns></returns>
        public bool Match(string possibleDDLName, string possibleKeyspaceName, INode possibleNode = null, bool checkDDLItems = false)
        {
            if(possibleNode != null)
            {
                if(!this.Node.Equals(possibleNode))
                {
                    return false;
                }
            }

            if(!string.IsNullOrEmpty(possibleDDLName) && this.TableViewIndex != null)
            {
                if(this.TableViewIndex.Name == possibleDDLName)
                {
                    return string.IsNullOrEmpty(possibleKeyspaceName)
                                ? true
                                : (this.Keyspace == null ? true : this.Keyspace.Name == possibleKeyspaceName);
                }
            }

            if(!string.IsNullOrEmpty(possibleKeyspaceName) && this.Keyspace != null)
            {
                return this.Keyspace.Name == possibleKeyspaceName;
            }

            if(checkDDLItems
                    && (!string.IsNullOrEmpty(possibleDDLName) || !string.IsNullOrEmpty(possibleKeyspaceName))
                    && this.DDLItems.HasAtLeastOneElement())
            {
                return this.DDLItems.Any(d => (string.IsNullOrEmpty(possibleKeyspaceName) ? true : d.Keyspace.Name == possibleKeyspaceName)
                                                && (string.IsNullOrEmpty(possibleDDLName) ? true : d.Name == possibleDDLName));
            }

            return false;
        }

#endregion

        /// <summary>
        /// If true, event is a temp event and cannot be associated to a node.
        /// </summary>
        public bool TempEvent { get; }

        public void UpdateEndingTimeStamp(DateTime eventTimeLocalEnd, bool checkEndTime = true, bool forceUpdate = false)
        {
            var newEndTime = new DateTimeOffset(eventTimeLocalEnd, this.EventTime.Offset);

            if (forceUpdate || !this.EventTimeEnd.HasValue || (checkEndTime && newEndTime > this.EventTimeEnd))
            {
                this.EventTimeEnd = newEndTime;
                this.Class &= ~EventClasses.Orphaned;
                if (this.Duration.HasValue || this.EventTimeBegin.HasValue)
                {
                    this.Duration = this.EventTimeEnd - this.EventTimeBegin;
                }
            }
        }

        public void UpdateDDLItems(IKeyspace newKeyspace, IDDLStmt newTableIndexView, IEnumerable<IDDLStmt> newDDLs)
        {
            if(newTableIndexView != null)
            {
                this._keyspace = null;
                this.TableViewIndex = newTableIndexView;
            }

            if(newKeyspace != null && this.TableViewIndex == null)
            {
                this._keyspace = newKeyspace;
            }

            if(newDDLs != null)
            {
                this.DDLItems = newDDLs;
            }
        }

        public bool FixSessionEvent(LogCassandraEvent beginendEvent)
        {
            bool bResult = false;

            if (this.Match(beginendEvent, true, false))
            {
                if ((this.Type & EventTypes.SessionItem) == EventTypes.SessionItem
                        && ((beginendEvent.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin
                                || (beginendEvent.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                                || (beginendEvent.Type & EventTypes.SessionBeginOrItem) == EventTypes.SessionBeginOrItem)
                        && !this.ParentEvents.Any(i => i.Equals(beginendEvent)))
                {
                    if (this.TableViewIndex == null)
                    {
                        this.TableViewIndex = beginendEvent.TableViewIndex;
                    }
                    if (this.Keyspace == null)
                    {
                        this._keyspace = beginendEvent.Keyspace;
                    }

                    if (this.ParentEvents.IsEmpty())
                    {
                        this.ParentEvents = new List<IMMLogValue>() { new Common.Patterns.Collections.MemoryMapped.MMValue<ILogEvent>(beginendEvent) };
                    }
                    else
                    {
                        this.ParentEvents = this.ParentEvents
                                                    .Append(new Common.Patterns.Collections.MemoryMapped.MMValue<ILogEvent>(beginendEvent))
                                                    .OrderBy(p => p.GetValue(MemoryMapperElementCreationTypes.ComparableView).EventTimeLocal);
                    }

                    this.SessionTieOutId = beginendEvent.SessionTieOutId;
                    this.Class &= ~EventClasses.Orphaned;
                    this.Id = beginendEvent.Id;

                    beginendEvent.UpdateEndingTimeStamp(this.EventTimeLocal);
                    if(!this.EventTimeBegin.HasValue)
                    {
                        this.EventTimeBegin = beginendEvent.EventTimeBegin;
                        if (this.Duration.HasValue || this.EventTimeEnd.HasValue)
                        {
                            this.Duration = this.EventTimeEnd - this.EventTimeBegin;
                        }
                    }

                    bResult = true;
                }
            }

            return bResult;
        }

        public void MarkAsOrphaned()
        {
            this.Class |= EventClasses.Orphaned;
        }

        public void SetException(string topLevelException, IEnumerable<string> exceptionPath, bool markAsException = true)
        {
            if(markAsException)
                this.Class |= EventClasses.Exception;
            if(!string.IsNullOrEmpty(topLevelException))
                this.Exception = topLevelException;
            if(exceptionPath != null && exceptionPath.HasAtLeastOneElement())
                this.ExceptionPath = exceptionPath;
        }

        public void SetException(string topLevelException, IEnumerable<string> exceptionPath, IReadOnlyDictionary<string, object> additionalProperties, bool markAsException = true)
        {
            this.SetException(topLevelException, exceptionPath, markAsException);

            if(additionalProperties != null && additionalProperties.HasAtLeastOneElement())
            {
                if(this.LogProperties == null || this.LogProperties.IsEmpty())
                {
                    this.LogProperties = additionalProperties;
                }
                else
                {
                    var newLogProperties = new Dictionary<string, object>(this.LogProperties.ToDictionary(k => k.Key, v => v.Value));
                    var elementsAdded = false;

                    foreach(var kv in additionalProperties)
                    {
                        if(newLogProperties.ContainsKey(kv.Key))
                        {
                            if(kv.Value is IEnumerable<object>)
                            {
                                var currentValue = newLogProperties[kv.Key];
                                var valueList = currentValue is IEnumerable<object>
                                                    ? new List<object>((IEnumerable<object>)currentValue)
                                                    : new List<object>() { currentValue };
                                var newElement = false;

                                foreach (var item in (IEnumerable<object>)kv.Value)
                                {
                                    if (!valueList.Contains(item))
                                    {
                                        valueList.Add(item);
                                        newElement = true;
                                    }              
                                }

                                if (newElement)
                                {
                                    newLogProperties[kv.Key] = valueList;
                                    elementsAdded = true;
                                }
                            }
                            else
                            {
                                var currentValue = newLogProperties[kv.Key];
                                var valueList = currentValue is IEnumerable<object>
                                                    ? new List<object>((IEnumerable<object>)currentValue)
                                                    : new List<object>() { currentValue };

                                if (!valueList.Contains(kv.Value))
                                {
                                    valueList.Add(kv.Value);
                                    newLogProperties[kv.Key] = valueList;
                                    elementsAdded = true;
                                }
                            }
                        }
                        else
                        {
                            newLogProperties.Add(kv.Key, kv.Value);
                            elementsAdded = true;
                        }
                    }                   
                    if(elementsAdded)
                        this.LogProperties = newLogProperties;
                }
            }            
        }



#region IEquatable
        public bool Equals(Guid other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.Id == other;
        }

        public bool Equals(IEvent other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.Source == other.Source && this.Id == other.Id;
        }

        public override bool Equals(object other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;
            if (other is IEvent) return this.Equals((IEvent)other);
            if (other is Guid) return this.Equals((Guid)other);

            return false;
        }

#endregion

#region Overrides

        [JsonProperty(PropertyName="HashCode")]
        private int _hashcode = 0;
        public override int GetHashCode()
        {
            unchecked
            {
                if (this._hashcode != 0) return this._hashcode;

                return this._hashcode = (this.Node.GetHashCode()
                                            + (this.SessionTieOutId == null ? 0 : this.SessionTieOutId.GetHashCode())
                                            + this.EventTimeLocal.GetHashCode()) * 31
#if DEBUG
                                            + this.LogMessage.GetHashCode();
#else
                                            + this.LogMessage;
#endif
            }
        }

        public override string ToString()
        {
            return string.Format("{0}{{{1}, {2}, {3}, {4}, {5}, {6:yyyy-MM-dd HH\\:mm\\:ss.fffzz}, {7:yyyy-MM-dd HH\\:mm\\:ss.fffzz}, {8}, {9}}}",
                                    this.Source,
                                    this.Node.Id,
                                    this.Id,
                                    this.Type,
                                    this.Class,
                                    this.TableViewIndex?.FullName ?? this.Keyspace?.FullName,
                                    this.EventTimeBegin.HasValue ? this.EventTimeBegin.Value : this.EventTime,
                                    this.EventTimeEnd,
                                    this.Items,
                                    this.ParentEvents?.Count());
        }

#endregion
    }
}
