using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common.Patterns.TimeZoneInfo;

namespace DSEDiagnosticLibrary
{
    [Flags]
    public enum EventTypes
    {
        Unkown = 0,
        /// <summary>
        /// Only one occurrence like an exception, etc.
        /// </summary>
        SingleInstance = 0x0001,
        SessionElement = 0x0002,
        /// <summary>
        /// A Session log item that is part of group of items that make up a session (e.g., compaction).
        /// This log item is not the start or end of this session.
        /// This log entry will use the associated SessionBegin&apos;s log id.
        /// If the start is not associated for this item it will be marked as orphaned.
        /// </summary>
        SessionItem = SessionElement | 0x0004,
        /// <summary>
        /// A Session log item that is part of group of items that make up a session (e.g., compaction).
        /// This log entry marks the beginning (start) of the session and is tracked. All entries between SessionBegin and SessionEnd are considered children of this entry.
        /// </summary>
        SessionBegin = SessionItem | 0x0008,
        /// <summary>
        /// A Session log item that is part of group of items that make up a session (e.g., compaction).
        /// This log entry marks the end of the session.
        /// </summary>
        SessionEnd = SessionItem | 0x0010,
        /// <summary>
        /// A special option used to determined if the log entry already has an associated SessionBegin.
        /// If the entry does not, it will be marked as a SessionBegin, otherwise it will be marked as SessionItem.
        /// </summary>
        /// <seealso cref="SessionBegin"/>
        /// <seealso cref="SessionItem"/>
        SessionBeginOrItem = SessionBegin | 0x0020,
        /// <summary>
        /// If not within a session the log entry will be ignored
        /// </summary>
        SessionIgnore = SessionItem | 0x0100,
        /// <summary>
        /// Defines a session based on the ending event&apos;s duration. A Session Begin Timestamp is generated based on this event&apos;s timestamp minus the duration.
        /// </summary>
        SessionDefinedByDuration = SessionItem | 0x0008 | 0x0010 | 0x0200,
        /// <summary>
        /// Defined a session that spans a time range. It will have a begin and end timestamp. Typically this is considered one event.
        /// </summary>
        SessionSpan = SessionItem | 0x0008 | 0x0010 | 0x0400,
        /// <summary>
        ///  Statistical data that is typically aggregated over some time period (e.g., node uptime, Log Period, etc.)
        /// </summary>
        AggregateData = 0x1000,
        /// <summary>
        ///  Statistical data that is typically derived by this application or another module (not from nodetool or dsetool)
        /// </summary>
        AggregateDataDerived = AggregateData | 0x2000,
        /// <summary>
        ///  Statistical data that is typically derived by a DSE/C* tool like nodetool or dsetool
        /// </summary>
        AggregateDataTool = AggregateData | 0x4000,

        /// <summary>
        /// If defined this event is an exception item
        /// </summary>
        ExceptionElement = 0x10000,

        /// <summary>
        /// Defines an Exception Instance
        /// </summary>
        ExceptionInstance = SingleInstance | ExceptionElement,
        SessionException = SessionItem | ExceptionElement
    }

    [Flags]
    public enum EventClasses : long
    {
        Unknown = 0,
        Information     = 0x0001,
        Warning         = 0x0002,
        Error           = 0x0004,
        Exception       = 0x0008,
        Fatal           = 0x0010,
        Compaction      = 0x0020,
        AntiCompaction  = 0x0040,
        Memtable        = 0x0080,
        GC              = 0x0100,
        Pause           = 0x0200,
        Repair          = 0x0400,
        Drops           = 0x0800,
        Performance     = 0x1000,
        Stats           = 0x2000,
        Orphaned        = 0x4000,
        HintHandOff     = 0x8000,
        DataCenter      = 0x10000,
        Node            = 0x20000,
        Keyspace        = 0x40000,
        TableViewIndex  = 0x80000,
        Config          = 0x100000,
        Detection       = 0x200000,
        NotHandled      = 0x400000,
        Pools           = 0x800000,
        Caches          = 0x1000000,
        Solr            = 0x2000000,
        Flush           = 0x4000000,
        Commit          = 0x8000000,
        Change          = 0x10000000,
        Schema          = 0x20000000,
        Shard           = 0x40000000,
        DataModel       = 0x80000000,
        Partition       = 0x100000000,
        Tombstone       = 0x200000000,
        Query           = 0x400000000,
        Gossip          = 0x800000000,
        Batches         = 0x1000000000,
        Prepares        = 0x2000000000,
        Authenticator   = 0x4000000000,
        Device          = 0x8000000000,
        TimeOut         = 0x10000000000,
        MetaData        = 0x20000000000,
        SSTable         = 0x40000000000,
        Row             = 0x80000000000,
        FileSystem      = 0x100000000000,
        Message         = 0x200000000000,
        Unavailable     = 0x400000000000,
        Network         = 0x800000000000,
        Requests        = 0x1000000000000,
        Terminate       = 0x2000000000000,
        Close           = 0x4000000000000,
        Session         = 0x8000000000000,
        TokenRange      = 0x10000000000000,
        GCStats = GC | Stats,
        MemtableFlush = Memtable | Flush,
        SolrHardCommit = Solr | Commit,
        SolrExpiredCols = Solr | Tombstone,
        NodeStats = Node | Stats,
        KeyspaceStats = Keyspace | Stats,
        TableViewIndexStats = TableViewIndex | Stats,
        KeyspaceTableViewIndexStats = Keyspace | TableViewIndexStats,
        PerformanceStats = Performance | Stats,
        DataModelStats = DataModel | Stats,
        NodeDetection = Node | Detection,
        StatusTypes = Information | Warning | Error | Exception | Fatal | Orphaned,
        LogTypes = Information | Warning | Error | Fatal
    }

	public interface IEvent : IParsed, IEquatable<Guid>, IEquatable<IEvent>, IComparable<IEvent>
    {
        EventTypes Type { get; }
        EventClasses Class { get; }
        /// <summary>
        /// e.g., Hint, Tombstone, etc.
        /// </summary>
        string SubClass { get; }

        /// <summary>
        /// If not defined one is generated. For repair session this should by the guid from the log
        /// For Aggregate Data this is for the complete set/collection (e.g., CFStats for a node)
        /// </summary>
        Guid Id { get; }
        /// <summary>
        /// Events associated with this event so that nesting
        /// </summary>
        IEnumerable<IEvent> ParentEvents { get; }
        IZone TimeZone { get; }
        /// <summary>
        /// Time event Occurred (e.g., This would be the logged time)
        /// </summary>
        DateTimeOffset EventTime { get; }
        DateTime EventTimeLocal { get; }
        /// <summary>
        /// For a timespan event, this would be the beginning timestamp.
        /// </summary>
        DateTimeOffset? EventTimeBegin { get; }
        /// <summary>
        /// For a timespan event, this would be the ending timestamp.
        /// </summary>
        DateTimeOffset? EventTimeEnd { get; }
        TimeSpan? Duration { get; }

        DSEInfo.InstanceTypes Product { get; }
        IKeyspace Keyspace { get; }
        IDDLStmt TableViewIndex { get; }

        string AnalyticsGroup { get; }
        
        NodeStateChange.DetectedStates? NodeTransitionState { get; }
    }
}
