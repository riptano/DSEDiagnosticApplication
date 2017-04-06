using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public sealed class LogCassandraEvent : IEvent
    {

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
        public IList<Guid> ParentEvents { get; }
        /// <summary>
        /// Time event Occurred (e.g., This would be the logged time)
        /// </summary>
        public DateTimeOffset EventTime { get; private set; }
        public DateTime EventTimeLocal { get; private set; }
        public DateTimeOffset EventTimeDurationStart { get; private set; }
        public TimeSpan Duration { get; private set; }

        private IKeyspace _keyspace = null;
        public IKeyspace Keyspace { get { return this._keyspace ?? this.Table?.Keyspace; } }
        public ICQLTable Table { get; private set; }
        #endregion
    }
}
