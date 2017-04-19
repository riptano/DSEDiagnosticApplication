using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Patterns.TimeZoneInfo;

namespace DSEDiagnosticLibrary
{
    public enum EventTypes
    {
        Unkown = 0,
        /// <summary>
        /// Only one occurrence like an exception, etc.
        /// </summary>
        SingleInstance,
        /// <summary>
        /// Overtime like compaction, GC, repairs, etc.
        /// </summary>
        SessionItem = 0x0002,
        SessionBegin = 0x0002 | 0x0004,
        SessionEnd = 0x0002 | 0x0008
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
        GCStats = 0x2000
    }

	public interface IEvent : IParsed
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
        IEnumerable<Guid> ParentEvents { get; }
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
