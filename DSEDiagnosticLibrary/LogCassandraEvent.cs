using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Patterns.TimeZoneInfo;

namespace DSEDiagnosticLibrary
{
    public sealed class LogCassandraEvent : IEvent
    {
        static readonly IReadOnlyDictionary<string, object> EmptyLogProperties = new System.Collections.ObjectModel.ReadOnlyDictionary<string, object>(new Dictionary<string, object>(0));
        static readonly IEnumerable<string> EmptySSTables = Enumerable.Empty<string>();
        static readonly IEnumerable<IDDLStmt> EmptyDDLItems = Enumerable.Empty<IDDLStmt>();
        static readonly IEnumerable<INode> EmptyAssocatedNodes = Enumerable.Empty<INode>();
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
                                    IEnumerable<Guid> parentEvents = null,
                                    IZone timeZone = null,
                                    IKeyspace keyspace = null,
                                    IDDLStmt tableviewindexItem = null,
                                    TimeSpan? duration = null,
                                    IReadOnlyDictionary<string, object> logProperties = null,
                                    IEnumerable<string> ssTables = null,
                                    IEnumerable<IDDLStmt> ddlItems = null,
                                    IEnumerable<INode> assocatedNodes = null,
                                    IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null,
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
            this.ParentEvents = parentEvents ?? Enumerable.Empty<Guid>();
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

            if(this.Type == EventTypes.SessionBegin)
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
                                   IEnumerable<Guid> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null,
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
                                   IEnumerable<Guid> parentEvents = null,
                                   IZone timeZone = null,
                                   IKeyspace keyspace = null,
                                   IDDLStmt tableviewindexItem = null,
                                   IReadOnlyDictionary<string, object> logProperties = null,
                                   IEnumerable<string> ssTables = null,
                                   IEnumerable<IDDLStmt> ddlItems = null,
                                   IEnumerable<INode> assocatedNodes = null,
                                   IEnumerable<DSEInfo.TokenRangeInfo> tokenRanges = null,
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
                        product)
        {
            this.EventTimeBegin = logLocalBeginTime;
            this.EventTimeEnd = this.EventTime;
            this.Duration = this.EventTimeEnd.Value - this.EventTimeBegin.Value;
        }

        #region IParsed
        public SourceTypes Source { get; private set; }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this.Node?.Cluster; } }
        public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
        public INode Node { get; private set; }
        public int Items { get; private set; }
        public uint LineNbr { get; private set; }

        #endregion

        #region IEvent
        public EventTypes Type { get; private set; }
        public EventClasses Class { get; private set; }
        /// <summary>
        /// e.g., Hint, Tombstone, etc.
        /// </summary>
        public string SubClass { get; private set; }

        /// <summary>
        /// If not defined one is generated. For repair session this should by the guid from the log
        /// </summary>
        public Guid Id { get; private set; }
        /// <summary>
        /// Events associated with this event so that nesting
        /// </summary>
        public IEnumerable<Guid> ParentEvents { get; }
        public IZone TimeZone { get; }
        /// <summary>
        /// Time event Occurred with time zone offset (e.g., This would be the logged time)
        /// </summary>
        public DateTimeOffset EventTime { get; private set; }
        public DateTime EventTimeLocal { get; private set; }
        public DateTimeOffset? EventTimeBegin { get; private set; }
        public DateTimeOffset? EventTimeEnd { get; private set; }
        public TimeSpan? Duration { get; private set; }

        private IKeyspace _keyspace = null;
        public IKeyspace Keyspace { get { return this._keyspace ?? this.TableViewIndex?.Keyspace; } }
        public IDDLStmt TableViewIndex { get; private set; }

        public DSEInfo.InstanceTypes Product { get; private set; }
        #endregion

        public IReadOnlyDictionary<string, object> LogProperties { get; private set; }
        public string LogMessage { get; private set; }
        public IEnumerable<string> SSTables { get; private set; }
        public IEnumerable<IDDLStmt> DDLItems { get; private set; }
        public IEnumerable<INode> AssociatedNodes { get; private set; }
        public IEnumerable<DSEInfo.TokenRangeInfo> TokenRanges { get; private set; }
        public string Exception { get; private set; }
        public IEnumerable<string> ExceptionPath { get; private set; }

        public void UpdateEndingTimeStamp(DateTime eventTimeLocalEnd)
        {
            this.EventTimeEnd = new DateTimeOffset(eventTimeLocalEnd, this.EventTime.Offset);

            if(!this.Duration.HasValue)
            {
                this.Duration = this.EventTimeEnd - this.EventTimeBegin;
            }
        }
    }
}
