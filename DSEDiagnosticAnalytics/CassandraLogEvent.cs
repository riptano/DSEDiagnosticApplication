using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Newtonsoft.Json;
using Common;
using CPC = Common.Patterns.Collections;
using CPCM = Common.Patterns.Collections.MemoryMapped;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticAnalytics
{
    public sealed class CassandraLogEvent : AnalyzeData, IDisposable
    {
        static readonly IEnumerable<int> EmptyReconciliationRefs = Enumerable.Empty<int>();

        public CassandraLogEvent(DSEDiagnosticFileParser.file_cassandra_log4net associatedEventParser)
            : base(associatedEventParser.Node, associatedEventParser.CancellationToken)
        {
            
        }

        public static void LogEventCreationCallBack(DSEDiagnosticFileParser.file_cassandra_log4net sender, DSEDiagnosticFileParser.file_cassandra_log4net.LogProcessingCreationArgs eventArgs)
        {
            var logEvtAnalytics = new CassandraLogEvent(sender);

            sender.OnLogEvent += logEvtAnalytics.LogEventCallBack;

            if(Logger.Instance.IsDebugEnabled)
            {
                Logger.Instance.DebugFormat("Registering CassandraLogEvent Analytics to LogFile {0}", sender.ShortFilePath);
            }
        }

        #region Stats

        /// <summary>
        /// An entry represents an log generated alert
        /// </summary>
        public sealed class LogAlertStat : DSEDiagnosticLibrary.IAggregatedStats, DSEDiagnosticLibrary.ILogEvent
        {
            static readonly Guid GuidValue = new Guid();

            [Flags]
            public enum OverLappingTypes
            {
                Normal = 0,
                /// <summary>
                /// There is a gap in time between this entry and the prior
                /// </summary>
                Gap = 0x1,
                /// <summary>
                /// This entry is a duplicate based on the prior entry
                /// </summary>
                Duplicated = 0x2,
                /// <summary>
                /// This entry is a overlapping based on the prior entry
                /// </summary>
                OverLapping = 0x4,
                /// <summary>
                /// This entry represents a series of continuous log files for a node (no gaps between log files)
                /// </summary>
                Continuous = 0x8,
                /// <summary>
                /// This entry represents a log files that meets or exceeds LogFileInfoAnalysisContinousEventInDays property for a node
                /// </summary>
                DurationThresholdMet = 0x10,
                /// <summary>
                /// This entry represents a series of continuous log files that meets or exceeds LogFileInfoAnalysisContinousEventInDays property for a node (no gaps between log files)
                /// </summary>
                ContinuousThresholdMet = Continuous | DurationThresholdMet
            }

            public LogAlertStat(INode node,
                                IFilePath logFilePath
                                )
            {
                this.Node = node;
                this.Path = logFilePath;
               
            }

            public LogAlertStat(CPC.MemoryMapper.ReadElement readView, CPC.MemoryMapperElementCreationTypes instanceType)
            {

            }

            #region IParsed Members

            public SourceTypes Source { get; } = SourceTypes.CassandraLog;
            /// <summary>
            /// LogFile Path. Is null when OverLappingTypes.Continous.
            /// </summary>
            public IPath Path { get; }
            public Cluster Cluster { get { return this.Node.Cluster; } }
            public IDataCenter DataCenter { get { return this.Node.DataCenter; } }
            public INode Node { get; }
            public int Items { get; } = 0;
            public uint LineNbr { get; } = 0;

            #endregion

            #region IEvent

            public EventTypes Type { get; } = EventTypes.AggregateData;
            public EventClasses Class { get; } = EventClasses.Node | EventClasses.Stats | EventClasses.Information;
            public string SubClass { get; } = "LogRangeStats";
            public Guid Id { get; } = GuidValue;
            [JsonIgnore]
            public IEnumerable<IEvent> ParentEvents { get; } = null;
            /// <summary>
            /// Node&apos;s timezone (log&apos;s timestamp time zone)
            /// </summary>
            public Common.Patterns.TimeZoneInfo.IZone TimeZone { get { return this.Node.Machine.TimeZone; } }
            /// <summary>
            ///
            /// </summary>
            public DateTimeOffset EventTime { get; }
            public DateTime EventTimeLocal { get; }
            /// <summary>
            /// Log&apos;s starting time
            /// </summary>
            public DateTimeOffset? EventTimeBegin { get; }
            /// <summary>
            /// Log&apos;s ending time
            /// </summary>
            public DateTimeOffset? EventTimeEnd { get; }
            /// <summary>
            /// Log&apos;s duration
            /// </summary>
            public TimeSpan? Duration { get; }

            public DSEInfo.InstanceTypes Product { get { return this.Node.DSE.InstanceType; } }
            public IKeyspace Keyspace { get; } = null;
            public IDDLStmt TableViewIndex { get; } = null;

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

            #endregion

            #region IAggregatedStats

            [JsonIgnore]
            public IDictionary<string, object> Data { get; } = null;

            public IAggregatedStats AssociateItem(string key, object value)
            {
                throw new NotImplementedException();
            }

            [JsonIgnore]
            public IEnumerable<int> ReconciliationRefs { get; } = EmptyReconciliationRefs;

            #endregion

            #region ILogEvent

            public string SessionTieOutId => throw new NotImplementedException();

            public IReadOnlyDictionary<string, object> LogProperties => throw new NotImplementedException();
            public string AnalyticsGroup { get; } = null;

#if DEBUG
            public string LogMessage { get; } = null;
#else
             public int LogMessage { get; } = 0;
#endif

            public IEnumerable<string> SSTables { get; } = Enumerable.Empty<string>();

            public IEnumerable<IDDLStmt> DDLItems { get; } = Enumerable.Empty<IDDLStmt>();

            public IEnumerable<INode> AssociatedNodes { get; } = Enumerable.Empty<INode>();

            public IEnumerable<TokenRangeInfo> TokenRanges { get; } = Enumerable.Empty<TokenRangeInfo>();

            public string Exception => throw new NotImplementedException();

            public IEnumerable<string> ExceptionPath => throw new NotImplementedException();

            IEnumerable<CPCM.IMMValue<ILogEvent>> ILogEvent.ParentEvents { get; } 

            public IAggregatedStats AssociateItem(int reference)
            {
                throw new NotImplementedException();
            }

            public bool Match(IEvent matchEvent, bool mustMatchNode = true, bool mustMatchKeyspaceDDL = true)
            {
                throw new NotImplementedException();
            }

            public bool Match(string possibleDDLName, string possibleKeyspaceName, INode possibleNode = null, bool checkDDLItems = false)
            {
                throw new NotImplementedException();
            }

            public void GetObjectData(CPC.MemoryMapper.WriteElement mmView)
            {
                throw new NotImplementedException();
            }

#endregion

        }

#endregion


#region Members

        public DSEDiagnosticFileParser.file_cassandra_log4net EventParser { get; }

        public IFilePath LogFilePath { get { return this.EventParser?.File; } }
        
        public void LogEventCallBack(DSEDiagnosticFileParser.file_cassandra_log4net sender, DSEDiagnosticFileParser.file_cassandra_log4net.LogEventArgs eventArgs)
        {
            if (eventArgs.LogEvent == null) return;

            if (eventArgs.LogMessage.FileName == "StatusLogger.java")
            {
                if ((eventArgs.LogEvent.Class & EventClasses.Caches) != 0)
                {
                    eventArgs.AllowNodeAssocation = false;
                    return;
                }
            }

            this.CancellationToken.ThrowIfCancellationRequested();



        }

        public override IEnumerable<IAggregatedStats> ComputeStats()
        {
            var logStats = new List<LogAlertStat>();

            try
            {
                 
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Analyzing Cassandra Log Event Information Canceled");
            }

            return logStats;
        }

#endregion

#region Dispose Methods

        public bool Disposed { get; private set; }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
	        Dispose(true);
	        // This object will be cleaned up by the Dispose method.
	        // Therefore, you should call GC.SupressFinalize to
	        // take this object off the finalization queue
	        // and prevent finalization code for this object
	        // from executing a second time.
	        GC.SuppressFinalize(this);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the
        // runtime from inside the finalizer and you should not reference
        // other objects. Only unmanaged resources can be disposed.
        private void Dispose(bool disposing)
        {
	        // Check to see if Dispose has already been called.
	        if(!this.Disposed)
	        {
		
		        if(disposing)
		        {
			        // Dispose all managed resources.
			        if(this.EventParser != null)
                    {
                        this.EventParser.OnLogEvent -= this.LogEventCallBack;
                    }
		        }

		        //Dispose of all unmanaged resources

		        // Note disposing has been done.
		        this.Disposed = true;

	        }
        }
#endregion //end of Dispose Methods
    }

    public static class CassandraLogEventAggregate
    {
        public delegate LogEventGrouping AggregateGroupFunc(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents);

        public static LogEventGrouping DurationStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var durationStats = new LogEventGrouping.DurationStatItems(assocatedLogEvents);
            return durationStats.HasValue ? new LogEventGrouping(ref logEventGroup, durationStats) : null;
        }
       
        public static long IsPositive(this IReadOnlyDictionary<string,object> propTable, string key)
        {
            object value;

            if(propTable.TryGetValue(key, out value))
            {
                if (value.IsNumber())
                {
                    long num = (long)((dynamic)value);
                    if (num > 0) return num;
                }
                else if(value is UnitOfMeasure)
                {
                    long num = (long)((UnitOfMeasure)value).NormalizedValue.Value;
                    if (num > 0) return num;
                }
            }
            return -1;
        }

        public static long IsPositiveZero(this IReadOnlyDictionary<string, object> propTable, string key)
        {
            object value;

            if (propTable.TryGetValue(key, out value))
            {
                if (value.IsNumber())
                {
                    long num = (long)((dynamic)value);
                    if (num >= 0) return num;
                }
                else if (value is UnitOfMeasure)
                {
                    long num = (long)((UnitOfMeasure)value).NormalizedValue.Value;
                    if (num >= 0) return num;
                }
            }
            return -1;
        }

        public static long GetPropLongValue(this IReadOnlyDictionary<string, object> propTable, string key)
        {
            object value;

            if (propTable.TryGetValue(key, out value) && value.IsNumber())
            {
                return (long)((dynamic)value);                
            }
            return 0L;
        }

        public static decimal GetPropDecimalValue(this IReadOnlyDictionary<string, object> propTable, string key)
        {
            object value;

            if (propTable.TryGetValue(key, out value) && value.IsNumber())
            {
                return (decimal)((dynamic)value);               
            }
            return 0M;
        }

        public static decimal GetPropUOMValue(this IReadOnlyDictionary<string, object> propTable, string key)
        {
            object value;

            if (propTable.TryGetValue(key, out value))
            {
                if(value != null)
                {
                    if(value is UnitOfMeasure)
                    {
                        return ((UnitOfMeasure)value).NormalizedValue.Value;
                    }
                    else if(value.IsNumber())
                    {
                        return (decimal)((dynamic)value);
                    }
                }
            }

            return -1M;
        }

        public static long IsPositive(this IReadOnlyDictionary<string, object> propTable, string key, ref long lastValue)
        {
            object value;

            if (propTable.TryGetValue(key, out value) && value.IsNumber())
            {
                long num = (long)((dynamic)value);
                if (num > 0)
                {
                    if(lastValue == 0) { lastValue = num; return -1; }
                    if(lastValue == -1) { return lastValue = num; }

                    long tmp = lastValue;
                    lastValue = num;
                    return num - tmp;
                }
            }
            return lastValue = -1;
        }

        public static LogEventGrouping StatusLoggerPoolStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            long lastCompleted = 0;
            long lastAllTimeBlock = 0;

            var poolPendingItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbrpending")).Where(i => i > 0);
            var poolCompletedItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbrcompleted", ref lastCompleted)).Where(i => i > 0);
            var poolBlockedItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbrblocked")).Where(i => i > 0);
            var poolBlockedAllTimeItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbralltimeblocked", ref lastAllTimeBlock)).Where(i => i > 0);

            return poolBlockedAllTimeItems.IsEmpty() && poolBlockedItems.IsEmpty() && poolCompletedItems.IsEmpty() && poolPendingItems.IsEmpty() 
                    ? null
                    : new LogEventGrouping(ref logEventGroup,
                                            assocatedLogEvents,
                                            poolPendingItems,
                                            poolCompletedItems,
                                            poolBlockedItems,
                                            poolCompletedItems);
        }

        public static LogEventGrouping StatusLoggerMemTblOPSStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {            
            var poolOPSItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("memtable")).Where(i => i > 0);
            var poolSizeItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("datasize")).Where(i => i > 0);
            
            return poolOPSItems.IsEmpty() && poolSizeItems.IsEmpty()
                    ? null
                    :new LogEventGrouping(ref logEventGroup,
                                            assocatedLogEvents,
                                            poolOPSItems,
                                            poolSizeItems);
        }

        public static LogEventGrouping LargePartitionRowStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var partitionSizeItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("size")).Where(i => i >= 0);
           
            return partitionSizeItems.IsEmpty()
                    ? null
                    : new LogEventGrouping(ref logEventGroup,
                                            assocatedLogEvents,
                                            partitionSizeItems);
        }

        public static LogEventGrouping TombstoneStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var tombstoneItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositiveZero("tombstone")).Where(i => i >= 0);
            var liveItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositiveZero("live")).Where(i => i >= 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        liveItems,
                                        tombstoneItems);
        }

        public static LogEventGrouping GCStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {            
            var poolEdenFromItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("edenspacefrom")).Where(i => i >= 0);
            var poolEdenToItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("edenspaceto")).Where(i => i >= 0);
            var poolOldGenFromItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("oldgenfrom")).Where(i => i >= 0);
            var poolOldGenToItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("oldgento")).Where(i => i >= 0);
            var poolSurvivorFromItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("survivorspacefrom")).Where(i => i >= 0);
            var poolSurvivorToItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("survivorspaceto")).Where(i => i >= 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        poolEdenFromItems,
                                        poolEdenToItems,
                                        poolOldGenFromItems,
                                        poolOldGenToItems,
                                        poolSurvivorFromItems,
                                        poolSurvivorToItems);
        }

    }

    public struct LogEventGroup
    {
        public LogEventGroup(DateTimeRange aggregationDateTime,
                                IDataCenter dataCenter,
                                INode node,
                                IKeyspace keyspace,
                                IDDLStmt ddlStmt,
                                EventClasses classes,
                                string subClass,
                                string exception,
                                IEnumerable<string> exceptionPath)
            : this()
        {
            this.AggregationDateTime = aggregationDateTime.Min;
            this.DataCenterName = dataCenter?.Name;
            this.NodeName = node?.Id.NodeName();
            this.KeySpaceName = keyspace?.Name;
            this.TableName = ddlStmt?.Name;
            this.Class = (classes & ~EventClasses.LogTypes).ToString();
            this.Action = subClass;
            this.Exception = exception;
            this.Path = exceptionPath?.Join("=>", s => s);
        }

        public LogEventGroup(DateTimeRange aggregationDateTime, ILogEvent logEvent)
            : this()
        {
            this.AggregationDateTime = aggregationDateTime.Min;
            this.DataCenterName = logEvent.DataCenter?.Name;
            this.NodeName = logEvent.Node?.Id.NodeName();
            this.KeySpaceName = logEvent.Keyspace?.Name;
            this.TableName = logEvent.TableViewIndex?.Name;
            this.Class = (logEvent.Class & ~EventClasses.LogTypes).ToString();
            this.Action = logEvent.SubClass;
            this.Exception = logEvent.Exception;
            this.Path = logEvent.ExceptionPath?.Join("=>", s => s);
        }

        public readonly DateTime AggregationDateTime;
        public readonly string DataCenterName;
        public readonly string NodeName;
        public readonly string KeySpaceName;
        public readonly string TableName;
        public readonly string Class;
        public readonly string Action;
        public readonly string Exception;
        public readonly string Path;                                    
    }

    public sealed class LogEventGrouping
    {

        public static IEnumerable<LogEventGrouping> CreateLogEventGrouping(LogEventGroup logEventGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {            
            return assocatedLogEvents.GroupBy(i => i.AnalyticsGroup).AsParallel()
                        .Select(logEvtGrp =>
                        {
                            if (string.IsNullOrEmpty(logEvtGrp.Key))
                            {
                                return new LogEventGrouping(ref logEventGroup, logEvtGrp, true);
                            }

                            AggregateGroups aggregateGroup;

                            if(LibrarySettings.AggregateGroups.TryGetValue(logEvtGrp.Key, out aggregateGroup))
                            {
                                return aggregateGroup.AggregateGroupFunction(ref logEventGroup, logEvtGrp.Key, logEvtGrp);
                            }

                            Logger.Instance.WarnFormat("Aggregate Group \"{0}\" was not found in AggregateGroups using CassandraLogEventAggregate.DurationStats function", logEvtGrp.Key);
                            return new LogEventGrouping(ref logEventGroup, logEvtGrp, true);
                        })
                        .Where(i => i?.HasValue ?? false);            
        }

        /// <summary>
        /// General Events or events with no Aggregate Group
        /// </summary>
        /// <param name="groupKey"></param>
        /// <param name="assocatedLogEvents"></param>
        public LogEventGrouping(ref LogEventGroup groupKey, IEnumerable<ILogEvent> assocatedLogEvents, bool forceHasValue = false)
        {
            this.GroupKey = groupKey;
            this.DurationStats = new DurationStatItems(assocatedLogEvents);
            this.HasValue = forceHasValue ? true : this.DurationStats?.HasValue ?? false;
        }

        /// <summary>
        /// Duration
        /// </summary>
        /// <param name="groupKey"></param>
        /// <param name="durationStats"></param>
        public LogEventGrouping(ref LogEventGroup groupKey, DurationStatItems durationStats)
        {
            this.GroupKey = groupKey;
            this.DurationStats = durationStats;
            this.HasValue = this.DurationStats.HasValue;
        }

        /// <summary>
        /// For Pools
        /// </summary>
        /// <param name="groupKey"></param>
        /// <param name="pending"></param>
        /// <param name="completed"></param>
        /// <param name="blocked"></param>
        /// <param name="alltimeBlocked"></param>
        /// <param name="nbrOccurs"></param>
        public LogEventGrouping(ref LogEventGroup groupKey,
                                    IEnumerable<ILogEvent> assocatedLogEvents,
                                    IEnumerable<long> pending,
                                    IEnumerable<long> completed,
                                    IEnumerable<long> blocked,
                                    IEnumerable<long> alltimeBlocked)           
        {
            this.GroupKey = groupKey;
            this.PoolStats = new PoolStatItems(pending, completed, blocked, alltimeBlocked);
            this.HasValue = this.PoolStats.HasValue;
        }

        /// <summary>
        /// For GC
        /// </summary>
        /// <param name="groupKey"></param>
        /// <param name="assocatedLogEvents"></param>
        /// <param name="beforeYoung"></param>
        /// <param name="afterYoung"></param>
        /// <param name="beforeEden"></param>
        /// <param name="afterEden"></param>
        /// <param name="beforeOld"></param>
        /// <param name="afterOld"></param>
        /// <param name="beforeSurvivor"></param>
        /// <param name="afterSurvivor"></param>
        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                IEnumerable<decimal> beforeEden, IEnumerable<decimal> afterEden,
                                IEnumerable<decimal> beforeOld, IEnumerable<decimal> afterOld,
                                IEnumerable<decimal> beforeSurvivor, IEnumerable<decimal> afterSurvivor)
        {            
            this.GroupKey = groupKey;
            this.DurationStats = new DurationStatItems(assocatedLogEvents);
            this.GCStats = new GCStatItems(beforeEden, afterEden, beforeOld, afterOld, beforeSurvivor, afterSurvivor);
            this.HasValue = (this.DurationStats?.HasValue ?? false) || this.GCStats.HasValue;
        }

        /// <summary>
        /// Pool MemTable OPS/Size
        /// </summary>
        /// <param name="groupKey"></param>
        /// <param name="assocatedLogEvents"></param>
        /// <param name="ops"></param>
        /// <param name="size"></param>
        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                IEnumerable<long> ops,
                                IEnumerable<decimal> size)
        {
            this.GroupKey = groupKey;
            
            this.PoolMemTblOPSStats = new PoolMemTblOPSItems(ops, size);
            this.HasValue = this.PoolMemTblOPSStats.HasValue;
        }

        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                IEnumerable<decimal> size)
        {
            this.GroupKey = groupKey;

            this.LargePartitionRowStats = new LargePartitionRowItems(size);
            this.HasValue = this.LargePartitionRowStats.HasValue;
        }

        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                IEnumerable<long> liveCells,
                                IEnumerable<long> tombstones)
        {
            this.GroupKey = groupKey;

            this.TombstoneStats = new TombstoneItems(liveCells, tombstones);
            this.HasValue = this.TombstoneStats.HasValue;
        }

        public readonly LogEventGroup GroupKey;
        public readonly bool HasValue;

        public struct ItemStatsLong
        {
            public ItemStatsLong(IEnumerable<long> itemCollection)
                : this()
            {
                if (itemCollection.HasAtLeastOneElement())
                {
                    this.Max = itemCollection.Max();
                    this.Min = itemCollection.Min();
                    this.Mean = (decimal) itemCollection.Average();
                    this.StdDev = (decimal) itemCollection.StandardDeviationP();
                    this.Sum = itemCollection.Sum();
                    this.HasValue = true;
                }
            }
            
            public readonly bool HasValue;
            public readonly long Max;
            public readonly long Min;
            public readonly decimal Mean;
            public readonly decimal StdDev;
            public readonly long Sum;
        }

        public struct ItemStatsDecimal
        {
            public ItemStatsDecimal(IEnumerable<decimal> itemCollection)
                : this()
            {
                if (itemCollection.HasAtLeastOneElement())
                {
                    this.Max = itemCollection.Max();
                    this.Min = itemCollection.Min();
                    this.Mean = itemCollection.Average();
                    this.StdDev = (decimal) itemCollection.StandardDeviationP();
                    this.Sum = itemCollection.Sum();
                    this.HasValue = true;
                }
            }

            public ItemStatsDecimal(IEnumerable<long> itemCollection)
                : this()
            {
                if (itemCollection.HasAtLeastOneElement())
                {
                    this.Max = ((decimal) itemCollection.Max()) / 1000M;
                    this.Min = ((decimal) itemCollection.Min()) / 1000M;
                    this.Mean = (decimal) (itemCollection.Average() / 1000D);
                    this.StdDev = (decimal) (itemCollection.StandardDeviationP() / 1000D);
                    this.Sum = ((decimal)itemCollection.Sum()) / 1000M;
                    this.HasValue = true;
                }
            }

            public readonly bool HasValue;
            public readonly decimal Max;
            public readonly decimal Min;
            public readonly decimal Mean;
            public readonly decimal StdDev;
            public readonly decimal Sum;
        }

        public sealed class DurationStatItems
        {
            public DurationStatItems(IEnumerable<ILogEvent> assocatedLogEvents)
            {
                var durationCollection = assocatedLogEvents
                                            .Where(i => i.Duration.HasValue)
                                            .Select(e => (long)e.Duration.Value.TotalMilliseconds);

                if (durationCollection.HasAtLeastOneElement())
                {
                    this.Duration = new ItemStatsDecimal(durationCollection);
                    this.HasValue = this.Duration.HasValue;
                }                
            }

            public DurationStatItems(IEnumerable<long> duration)
            {               
                if (duration.HasAtLeastOneElement())
                {
                    this.Duration = new ItemStatsDecimal(duration);
                    this.HasValue = this.Duration.HasValue;
                }
            }

            public readonly bool HasValue;
            public readonly ItemStatsDecimal Duration;
            public readonly string UOM = "Seconds";
        }

        public readonly DurationStatItems DurationStats;
        
        public sealed class PoolStatItems
        {
            public PoolStatItems(IEnumerable<long> pending, IEnumerable<long> completed, IEnumerable<long> blocked, IEnumerable<long> alltimeBlocked)                
            {
                this.Pending = new ItemStatsLong(pending);
                this.Completed = new ItemStatsLong(completed);
                this.Blocked = new ItemStatsLong(blocked);
                this.AllTimeBlocked = new ItemStatsLong(alltimeBlocked);
                this.HasValue = this.Pending.HasValue | this.Completed.HasValue | this.Blocked.HasValue | this.AllTimeBlocked.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong Pending;
            public readonly ItemStatsLong Completed;
            public readonly ItemStatsLong Blocked;
            public readonly ItemStatsLong AllTimeBlocked;
            public readonly string UOM = "Transactions";
        }

        public readonly PoolStatItems PoolStats;

        public sealed class GCBeforeAfterItems
        {
            public GCBeforeAfterItems(IEnumerable<decimal> before, IEnumerable<decimal> after)              
            {
                this.Before = new ItemStatsDecimal(before);
                this.After = new ItemStatsDecimal(after);

                var nbrItems = before.Count();
                var difItems = new List<decimal>(nbrItems);

                for(int nIdx = 0; nIdx < nbrItems; ++nIdx)
                {
                    difItems.Add(after.ElementAt(nIdx) - before.ElementAt(nIdx));
                }
                this.Difference = new ItemStatsDecimal(difItems);
                this.HasValue = this.Before.HasValue || this.After.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsDecimal Before;
            public readonly ItemStatsDecimal After;
            public readonly ItemStatsDecimal Difference;            
        }

        public sealed class GCStatItems 
        {

            public GCStatItems(IEnumerable<decimal> beforeEden, IEnumerable<decimal> afterEden,
                                IEnumerable<decimal> beforeOld, IEnumerable<decimal> afterOld,
                                IEnumerable<decimal> beforeSurvivor, IEnumerable<decimal> afterSurvivor)               
            {               
                if(beforeEden != null && beforeEden.HasAtLeastOneElement())
                    this.Eden = new GCBeforeAfterItems(beforeEden, afterEden);
                if(beforeOld != null && beforeOld.HasAtLeastOneElement())
                    this.Old = new GCBeforeAfterItems(beforeOld, afterOld);
                if(beforeSurvivor != null && beforeSurvivor.HasAtLeastOneElement())
                    this.Survivor = new GCBeforeAfterItems(beforeSurvivor, afterSurvivor);
                this.HasValue = (this.Eden != null && this.Eden.HasValue)
                                    | (this.Old != null && this.Old.HasValue)
                                    | (this.Survivor != null && this.Survivor.HasValue);
            }

            public readonly bool HasValue;            
            public readonly GCBeforeAfterItems Eden;
            public readonly GCBeforeAfterItems Old;
            public readonly GCBeforeAfterItems Survivor;
            public readonly string UOM = "MiB";
        }

        public readonly GCStatItems GCStats;

        public sealed class PoolMemTblOPSItems
        {
            public PoolMemTblOPSItems(IEnumerable<long> ops, IEnumerable<decimal> size)
            {
                this.OPS = new ItemStatsLong(ops);
                this.Size = new ItemStatsDecimal(size);
                
                this.HasValue = this.OPS.HasValue || this.Size.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong OPS;
            public readonly ItemStatsDecimal Size;
            public readonly string UOM = "Operations/Second, Mib";
        }

        public readonly PoolMemTblOPSItems PoolMemTblOPSStats;

        public sealed class LargePartitionRowItems
        {
            public LargePartitionRowItems(IEnumerable<decimal> size)
            {                
                this.Size = new ItemStatsDecimal(size);

                this.HasValue =  this.Size.HasValue;
            }

            public readonly bool HasValue;            
            public readonly ItemStatsDecimal Size;
            public readonly string UOM = "Mib";
        }

        public readonly LargePartitionRowItems LargePartitionRowStats;

        public sealed class TombstoneItems
        {
            public TombstoneItems(IEnumerable<long> liveCells, IEnumerable<long> tombstones)
            {
                this.LiveCells = new ItemStatsLong(liveCells);
                this.Tombstones = new ItemStatsLong(tombstones);

                this.HasValue = this.LiveCells.HasValue || this.Tombstones.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong LiveCells;
            public readonly ItemStatsLong Tombstones;
            public readonly string UOM = "Cells";
        }

        public readonly TombstoneItems TombstoneStats;
    }
}
