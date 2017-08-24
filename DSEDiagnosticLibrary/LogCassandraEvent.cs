using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using Common.Patterns.TimeZoneInfo;

namespace DSEDiagnosticLibrary
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class LogCassandraEvent : IEvent
    {
        static readonly IReadOnlyDictionary<string, object> EmptyLogProperties = new System.Collections.ObjectModel.ReadOnlyDictionary<string, object>(new Dictionary<string, object>(0));
        static readonly IEnumerable<string> EmptySSTables = Enumerable.Empty<string>();
        static readonly IEnumerable<IDDLStmt> EmptyDDLItems = Enumerable.Empty<IDDLStmt>();
        static readonly IEnumerable<INode> EmptyAssocatedNodes = Enumerable.Empty<INode>();
        static readonly IEnumerable<IEvent> EmptyParents = Enumerable.Empty<IEvent>();
        static readonly IEnumerable<DSEInfo.TokenRangeInfo> EmptyTokenRanges = Enumerable.Empty<DSEInfo.TokenRangeInfo>();

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
                                    IEnumerable<IEvent> parentEvents = null,
                                    IZone timeZone = null,
                                    IKeyspace keyspace = null,
                                    IDDLStmt tableviewindexItem = null,
                                    TimeSpan? duration = null,
                                    IReadOnlyDictionary<string, object> logProperties = null,
                                    IEnumerable<string> ssTables = null,
                                    IEnumerable<IDDLStmt> ddlItems = null,
                                    IEnumerable<INode> assocatedNodes = null,
                                    IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null,
                                    string sessionTieOutId = null,
                                    DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra)
        {
            if (logFile == null) throw new ArgumentNullException("logFile cannot be null");
            if (node == null) throw new ArgumentNullException("node cannot be null");
            if (string.IsNullOrEmpty(logMessage)) throw new ArgumentNullException("logMessage cannot be null");

            this.Source = SourceTypes.CassandraLog;
            this.Id = string.IsNullOrEmpty(logId) ? Guid.NewGuid() : new Guid(logId);
            this.Path = logFile;
            this.Node = node;
            this.LogMessage = logMessage;
            this.LineNbr = lineNbr;
            this.Type = eventType;
            this.Class = logEventClass;
            this.SubClass = subClass;
            this.ParentEvents = parentEvents ?? EmptyParents;
            this.TimeZone = timeZone;
            this.EventTimeLocal = logLocalTime;
            this.EventTime = logLocalTimewOffset;
            this.Duration = duration;
            this._keyspace = keyspace;
            this.TableViewIndex = tableviewindexItem;
            this.Product = product;
            this.LogProperties = logProperties ?? EmptyLogProperties;
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
                                   IEnumerable<IEvent> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null,
                                   string sessionTieOutId = null,
                                   DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra)
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
                        product)
        {
            if (uomDuration == null) throw new ArgumentNullException("uomDuration cannot be null");

            this.Duration = (TimeSpan)uomDuration;
            this.EventTimeBegin = this.EventTime - this.Duration;
            this.EventTimeEnd = this.EventTime;
        }

        public LogCassandraEvent(IFilePath logFile,
                                   INode node,
                                   uint lineNbr,
                                   EventClasses logEventClass,
                                   DateTime logLocalTime,
                                   DateTimeOffset logLocalTimewOffset,
                                   DateTimeOffset logLocalBeginTime,
                                   string logMessage,
                                   EventTypes eventType,
                                   string logId = null,
                                   string subClass = null,
                                   IEnumerable<IEvent> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null,
                                   string sessionTieOutId = null,
                                   DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra)
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
                        product)
        {
            this.EventTimeBegin = logLocalBeginTime;
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
        /// <summary>
        /// Events associated with this event so that nesting
        /// </summary>
        public IEnumerable<IEvent> ParentEvents { get; private set; }
        [JsonProperty(PropertyName="TimeZone")]
        private string datamemberTimeZone
        {
            get { return this.TimeZone?.Name; }
            set { this.TimeZone = Common.TimeZones.Find(value); }
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
        #endregion


        public string SessionTieOutId { get; private set; }
        public IReadOnlyDictionary<string, object> LogProperties { get; }
        public string LogMessage { get; }
        public IEnumerable<string> SSTables { get; private set; }
        public IEnumerable<IDDLStmt> DDLItems { get; private set; }
        public IEnumerable<INode> AssociatedNodes { get; private set; }
        public IEnumerable<DSEInfo.TokenRangeInfo> TokenRanges { get; private set; }
        public string Exception { get; private set; }
        public IEnumerable<string> ExceptionPath { get; private set; }

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
                        this.ParentEvents = new List<IEvent>(this.ParentEvents) { beginendEvent };
                    }
                    else
                    {
                        this.ParentEvents = this.ParentEvents.Append(beginendEvent).OrderBy(p => p.EventTimeLocal);
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
                                            + this.LogMessage.GetHashCode();
            }
        }

        public override string ToString()
        {
            return string.Format("{0}{{{1}, {2}, {3}, {4}, {5}, {6:yyyy-MM-dd HH\\:mm\\:ss.fffzz}, {7:yyyy-MM-dd HH\\:mm\\:ss.fffzz}, {8}}}",
                                    this.Source,
                                    this.Node.Id,
                                    this.Id,
                                    this.Type,
                                    this.Class,
                                    this.TableViewIndex?.FullName ?? this.Keyspace?.FullName,
                                    this.EventTimeBegin.HasValue ? this.EventTimeBegin.Value : this.EventTime,
                                    this.EventTimeEnd,
                                    this.Items);
        }

        #endregion
    }
}
