using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        Duration
    }

    public enum EventClasses
    {
        Unknown = 0,
        Information,
        Warning,
        Error,
        Exception,
        Compaction,
        AntiCompaction,
        MemtableFlush,
        GC,
        Pause,
        Repair,
        Drops,
        Performance,
        GCStats
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
        IList<Guid> ParentEvents { get; }
        /// <summary>
        /// Time event Occurred (e.g., This would be the logged time)
        /// </summary>
        DateTimeOffset EventTime { get; }
        DateTime EventTimeLocal { get; }
        DateTimeOffset EventTimeDurationStart { get; }
        TimeSpan Duration { get; }

        IKeyspace Keyspace { get; }
        ICQLTable Table { get; }
    }
}
