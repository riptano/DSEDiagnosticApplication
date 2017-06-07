using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        SessionIgnore = SessionElement | 0x0100  
    }

    [Flags]
    public enum EventClasses
    {
        Unknown = 0,
        Information = 0x0001,
        Warning = 0x0002,
        Error = 0x0004,
        Exception = 0x0008,
        Fatal = 0x0010,
        Compaction = 0x0020,
        AntiCompaction = 0x0040,
        MemtableFlush = 0x0080,
        GC = 0x0100,
        Pause = 0x0200,
        Repair = 0x0400,
        Drops = 0x0800,
        Performance = 0x1000,
        GCStats = 0x2000,
        Orphaned = 0x4000,
        HintHandOff = 0x8000,
        NodeDetection = 0x10000,
        Config = 0x20000,
        
        StatusTypes = Information | Warning | Error | Exception | Fatal | Orphaned
    }

	public interface IEvent : IParsed, IEquatable<Guid>, IEquatable<IEvent>
	{
        EventTypes Type { get; }
        EventClasses Class { get; }
        /// <summary>
        /// e.g., Hint, Tombstone, etc.
        /// </summary>
        string SubClass { get; }

        /// <summary>
        /// If not defined one is generated. For repair session this should by the guid from the log
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
    }
}
