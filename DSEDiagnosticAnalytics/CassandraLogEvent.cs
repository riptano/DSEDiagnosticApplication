using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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
            sender.Tag = logEvtAnalytics;

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

            public string AnalyticsGroup { get; } = null;

            public NodeStateChange.DetectedStates? NodeTransitionState { get; } = null;

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

            public IAggregatedStats UpdateAssociateItem(string key, object value)
            {
                throw new NotImplementedException();
            }

            [JsonIgnore]
            public IEnumerable<int> ReconciliationRefs { get; } = EmptyReconciliationRefs;

            #endregion

            #region ILogEvent

            public string SessionTieOutId => throw new NotImplementedException();

            public IReadOnlyDictionary<string, object> LogProperties => throw new NotImplementedException();            

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

        public static CTS.Dictionary<Tuple<INode,EventClasses,int,int>, AggregatedStats> AggregatedStats = new CTS.Dictionary<Tuple<INode, EventClasses,int,int>, AggregatedStats>();

        public static CTS.Dictionary<INode, Tuple<GCStat,GCStat>> GCStats = new CTS.Dictionary<INode, Tuple<GCStat, GCStat>>();

        public static CTS.List<Tuple<INode, EventClasses, int, int, string>> AggregatedStatsReduced = new CTS.List<Tuple<INode, EventClasses, int, int, string>>();

        public readonly Regex NetworkExceptionMatchRegEx = new Regex(Properties.Settings.Default.NetworkExceptinMatchRegEx,
                                                                            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public static void CleanUp()
        {
            if (LibrarySettings.GCBackToBackToleranceMS > 0
                    || LibrarySettings.GCTimeFrameDetection > TimeSpan.Zero)
            {
                System.Threading.Tasks.Parallel.ForEach(GCStats, gcStat =>
                //foreach (var gcStat in GCStats)
                {
                    try
                    {
                        gcStat.Value.Item1.TestAddEvent(null);
                        gcStat.Value.Item2.TestAddEvent(null);
                    }
                    catch
                    { }
                }
                );
            }

            System.Threading.Tasks.Parallel.ForEach(AggregatedStatsReduced.DuplicatesRemoved(g => g), redecedStats =>
            //foreach (var redecedStats in AggregatedStatsReduced.DuplicatesRemoved(g => g))
            {
                if (AggregatedStats.TryGetValue(new Tuple<INode, EventClasses, int, int>(redecedStats.Item1,
                                                                                        redecedStats.Item2,
                                                                                        redecedStats.Item3,
                                                                                        redecedStats.Item4), out DSEDiagnosticLibrary.AggregatedStats aggStat))
                {
                    if (aggStat.Data.TryGetValue(redecedStats.Item5, out object dataValue))
                    {
                        var nbrItems = ((System.Collections.IList)dataValue).Count;

                        if (nbrItems >= LogAggNodeStatMbrLimitTake * 4)
                            AggregateStatMembers(aggStat.Path as IFilePath,
                                                    aggStat.Node,
                                                    aggStat.Class,
                                                    (IDDL)aggStat.TableViewIndex ?? aggStat.Keyspace,
                                                    redecedStats.Item5,
                                                    int.MaxValue,
                                                    (int)(nbrItems * Properties.Settings.Default.LogAggregationNodeStatMbrLimitTakeNItemsPercent),
                                                    (dynamic)dataValue,
                                                    redecedStats.Item3,
                                                    redecedStats.Item4,
                                                    aggStat);
                    }
                }
            }
            );

            GCStats.Clear();
            AggregatedStats.Clear();
            AggregatedStatsReduced.Clear();
        }

        private static bool AggregateStatMembers<T>(IFilePath filePath,
                                                        INode node,
                                                        EventClasses evtClass,
                                                        IDDL ddlStmt,
                                                        string attribName,
                                                        int maxAggregatedLimit,
                                                        int takeNItems,
                                                        IList<T> dataList,
                                                        int keyspaceHC,
                                                        int tableHC,
                                                        DSEDiagnosticLibrary.AggregatedStats useStat = null)
            where T : struct
        {
            var nbrItems = dataList.Count;

            if (nbrItems < takeNItems || nbrItems < maxAggregatedLimit) return false;

            var stat = useStat ?? new AggregatedStats(filePath,
                                                        node,
                                                        SourceTypes.CassandraLog,
                                                        EventTypes.AggregateDataTool,
                                                        evtClass,
                                                        ddlStmt);
            var statList = new List<T>();
            var orderedList = dataList.OrderBy(i => i);

            if (takeNItems > 0
                    && takeNItems * 4 < nbrItems)
            {                
                statList.AddRange(orderedList.Take(takeNItems).DuplicatesRemoved(g => g));
                statList.AddRange(orderedList.Skip((nbrItems / 2)
                                                    - (takeNItems / 2))
                                                .Take(takeNItems)
                                                .DuplicatesRemoved(g => g));
                statList.AddRange(orderedList.TakeLast(takeNItems).DuplicatesRemoved(g => g));
            }
            else
            {                
                statList.Add(orderedList.First());
                statList.Add(orderedList.Last());
                statList.Add(orderedList.ElementAt(nbrItems / 2));
            }

            stat.UpdateAssociateItem(attribName, statList);
            stat.UpdateAssociateItem(attribName + " Occurrences", nbrItems);

            AggregatedStatsReduced.TryAdd(new Tuple<INode, EventClasses, int, int, string>(node,
                                                                                            evtClass,
                                                                                            keyspaceHC,
                                                                                            tableHC,
                                                                                            attribName));
            return true;
        }

        static readonly int LogAggNodeStatMbrLimitTake = (int)(Properties.Settings.Default.LogAggregationNodeStatMbrLimit * Properties.Settings.Default.LogAggregationNodeStatMbrLimitTakeNItemsPercent);

        public void LogEventCallBack(DSEDiagnosticFileParser.file_cassandra_log4net sender, DSEDiagnosticFileParser.file_cassandra_log4net.LogEventArgs eventArgs)
        {
            if (eventArgs.LogEvent == null) return;

            this.CancellationToken.ThrowIfCancellationRequested();

            int keyspaceHC = eventArgs.LogEvent.Keyspace?.GetHashCode() ?? 0;
            int tableHC = eventArgs.LogEvent.TableViewIndex?.GetHashCode() ?? 0;

            if ((eventArgs.LogEvent.Class & EventClasses.Partition) == EventClasses.Partition
                    && eventArgs.LogEvent.TableViewIndex != null
                    && eventArgs.LogEvent.LogProperties.ContainsKey(Properties.StatPropertyNames.Default.LogLargePartition))
            {
                var partitionSize = (UnitOfMeasure)eventArgs.LogEvent.LogProperties[Properties.StatPropertyNames.Default.LogLargePartition];
                var largeRow = eventArgs.LogEvent.SubClass.EndsWith("row", StringComparison.InvariantCultureIgnoreCase);
                var attribName = largeRow ? Properties.StatPropertyNames.Default.RowLarge : Properties.StatPropertyNames.Default.PartitionLarge;
                var evtClass = EventClasses.Node | EventClasses.KeyspaceTableViewIndexStats;

                if (largeRow)
                    evtClass |= EventClasses.Row;
                else
                    evtClass |= EventClasses.Partition;

                var aggStat = AggregatedStats.GetOrAdd(new Tuple<INode, EventClasses, int, int>(sender.Node,
                                                                                                evtClass,
                                                                                                keyspaceHC,
                                                                                                tableHC), sndNode =>
                                                         {
                                                             var stat = new AggregatedStats(sender.File,
                                                                                                 sndNode.Item1,
                                                                                                 SourceTypes.CassandraLog,
                                                                                                 EventTypes.AggregateDataTool,
                                                                                                 sndNode.Item2,
                                                                                                 eventArgs.LogEvent.TableViewIndex);
                                                             //sndNode.Item1.AssociateItem(stat);
                                                             return stat;
                                                         });

                if (aggStat.Data.TryGetValue(attribName, out object dataValue))
                {
                    var dataList = (List<UnitOfMeasure>)dataValue;

                    if (AggregateStatMembers(sender.File,
                                                sender.Node,
                                                evtClass,
                                                eventArgs.LogEvent.TableViewIndex,
                                                attribName,
                                                Properties.Settings.Default.LogAggregationNodeStatMbrLimit,
                                                LogAggNodeStatMbrLimitTake,
                                                dataList,
                                                keyspaceHC,
                                                tableHC))
                    {
                        dataList.Clear();
                    }
                    dataList.Add(partitionSize);
                }
                else
                {
                    try
                    {
                        aggStat.AssociateItem(attribName, new List<UnitOfMeasure>() { partitionSize });
                    }
                    catch (System.ArgumentException)
                    {
                        if (aggStat.Data.TryGetValue(attribName, out dataValue))
                        {
                            ((List<UnitOfMeasure>)dataValue).Add(partitionSize);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }

            if ((eventArgs.LogEvent.Class & EventClasses.Tombstone) == EventClasses.Tombstone
                    && eventArgs.LogEvent.TableViewIndex != null
                    && eventArgs.LogEvent.LogProperties.ContainsKey(Properties.StatPropertyNames.Default.LogTombstones))
            {
                var evtClass = EventClasses.Tombstone | EventClasses.Node | EventClasses.KeyspaceTableViewIndexStats;

                var nodeStat = AggregatedStats.GetOrAdd(new Tuple<INode, EventClasses, int, int>(sender.Node,
                                                                                                evtClass,
                                                                                                keyspaceHC,
                                                                                                tableHC), sndNode =>
                {
                    var stat = new AggregatedStats(sender.File,
                                                        sndNode.Item1,
                                                        SourceTypes.CassandraLog,
                                                        EventTypes.AggregateDataTool,
                                                        sndNode.Item2,
                                                        eventArgs.LogEvent.TableViewIndex);
                    //sndNode.Item1.AssociateItem(stat);
                    return stat;
                });

                var tombstones = (long)((dynamic)eventArgs.LogEvent.LogProperties[Properties.StatPropertyNames.Default.LogTombstones]);

                if (nodeStat.Data.TryGetValue(Properties.StatPropertyNames.Default.TombstonesRead, out object dataValue))
                {
                    nodeStat.UpdateAssociateItem(Properties.StatPropertyNames.Default.TombstonesRead, (long)dataValue + tombstones);
                }
                else
                {
                    nodeStat.AssociateItem(Properties.StatPropertyNames.Default.TombstonesRead, tombstones);
                }

                if (eventArgs.LogEvent.LogProperties.TryGetValue(Properties.StatPropertyNames.Default.LogLiveCells, out dynamic reads))
                {
                    var readsValue = (long)reads;

                    //TombstoneLiveCellAttrib
                    if (nodeStat.Data.TryGetValue(Properties.StatPropertyNames.Default.TombstoneLiveCell, out dataValue))
                    {
                        nodeStat.UpdateAssociateItem(Properties.StatPropertyNames.Default.TombstoneLiveCell, (long)dataValue + readsValue);
                    }
                    else
                    {
                        nodeStat.AssociateItem(Properties.StatPropertyNames.Default.TombstoneLiveCell, readsValue);
                    }

                    decimal percent = 0;

                    if (tombstones > 0 || reads > 0)
                        percent = (decimal)tombstones / (((decimal)tombstones) + (decimal)reads);

                    if (nodeStat.Data.TryGetValue(Properties.StatPropertyNames.Default.TombstoneLiveCellRatio, out dataValue))
                    {
                        var dataList = (List<decimal>)dataValue;

                        if (AggregateStatMembers(sender.File,
                                                    sender.Node,
                                                    evtClass,
                                                    eventArgs.LogEvent.TableViewIndex,
                                                    Properties.StatPropertyNames.Default.TombstoneLiveCellRatio,
                                                    Properties.Settings.Default.LogAggregationNodeStatMbrLimit,
                                                    LogAggNodeStatMbrLimitTake,
                                                    dataList,
                                                    keyspaceHC,
                                                    tableHC))
                        {
                            dataList.Clear();
                        }

                        dataList.Add(percent);
                    }
                    else
                    {
                        nodeStat.AssociateItem(Properties.StatPropertyNames.Default.TombstoneLiveCellRatio, new List<decimal>() { percent });
                    }
                }
            }

            if ((eventArgs.LogEvent.Class & EventClasses.Compaction) == EventClasses.Compaction
                    && eventArgs.LogEvent.SubClass == Properties.StatPropertyNames.Default.LogInsufficientSpaceSubClass
                    && eventArgs.LogEvent.LogProperties.ContainsKey(Properties.StatPropertyNames.Default.LogInsufficientSpace))
            {
                var spaceRequired = (UnitOfMeasure)eventArgs.LogEvent.LogProperties[Properties.StatPropertyNames.Default.LogInsufficientSpace];
                var evtClass = EventClasses.Compaction | EventClasses.Node;

                if (eventArgs.LogEvent.TableViewIndex == null)
                {
                    if (eventArgs.LogEvent.Keyspace != null)
                    {
                        evtClass |= EventClasses.Keyspace;
                    }
                }
                else
                {
                    evtClass |= EventClasses.KeyspaceTableViewIndexStats;
                }

                var aggStat = AggregatedStats.GetOrAdd(new Tuple<INode, EventClasses, int, int>(sender.Node,
                                                                                                evtClass,
                                                                                                keyspaceHC,
                                                                                                tableHC), sndNode =>
                {
                    var stat = new AggregatedStats(sender.File,
                                                        sndNode.Item1,
                                                        SourceTypes.CassandraLog,
                                                        EventTypes.AggregateDataTool,
                                                        sndNode.Item2,
                                                        (IDDL)eventArgs.LogEvent.TableViewIndex ?? eventArgs.LogEvent.Keyspace);

                    //sndNode.Item1.AssociateItem(stat);
                    return stat;
                });

                if (aggStat.Data.TryGetValue(Properties.StatPropertyNames.Default.CompactionInsufficientSpace, out object dataValue))
                {
                    var dataList = (List<UnitOfMeasure>)dataValue;

                    if (AggregateStatMembers(sender.File,
                                                    sender.Node,
                                                    evtClass,
                                                    (IDDL)eventArgs.LogEvent.TableViewIndex ?? eventArgs.LogEvent.Keyspace,
                                                    Properties.StatPropertyNames.Default.CompactionInsufficientSpace,
                                                    Properties.Settings.Default.LogAggregationNodeStatMbrLimit,
                                                    LogAggNodeStatMbrLimitTake,
                                                    dataList,
                                                    keyspaceHC,
                                                    tableHC))
                    {
                        dataList.Clear();
                    }

                    dataList.Add(spaceRequired);
                }
                else
                {
                    try
                    {
                        aggStat.AssociateItem(Properties.StatPropertyNames.Default.CompactionInsufficientSpace, new List<UnitOfMeasure>() { spaceRequired });
                    }
                    catch (System.ArgumentException)
                    {
                        if (aggStat.Data.TryGetValue(Properties.StatPropertyNames.Default.CompactionInsufficientSpace, out dataValue))
                        {
                            ((List<UnitOfMeasure>)dataValue).Add(spaceRequired);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
            }

            if ((LibrarySettings.GCBackToBackToleranceMS > 0
                    || LibrarySettings.GCTimeFrameDetection > TimeSpan.Zero)
                    && (eventArgs.LogEvent.Class & EventClasses.GC) == EventClasses.GC
                    && ((eventArgs.LogEvent.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                            || (eventArgs.LogEvent.Type & EventTypes.SingleInstance) == EventTypes.SingleInstance)
                    && eventArgs.LogEvent.EventTimeEnd.HasValue)
            {
                var gcStat = GCStats.GetOrAdd(sender.Node, sndNode =>
                {
                    var stat = new Tuple<GCStat, GCStat>(new GCStat(sndNode, GCStat.GCStatTypes.BackToBack), new GCStat(sndNode, GCStat.GCStatTypes.TimeFrame));
                    return stat;
                });

                gcStat.Item1.TestAddEvent(eventArgs.LogEvent);
                gcStat.Item2.TestAddEvent(eventArgs.LogEvent);
            }

            if ((eventArgs.LogEvent.Class & EventClasses.Repair) == EventClasses.Repair)
            {
                if((eventArgs.LogEvent.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                        && eventArgs.LogEvent.LogProperties.TryGetValue("status", out object status)
                        && status != null
                        && status is string
                        && ((string)status).StartsWith("success"))
                {
                    eventArgs.LogEvent.Node.DSE.RepairServiceHasRan = true;
                }                    
            }

            if ((eventArgs.LogEvent.Class & EventClasses.Exception) == EventClasses.Exception
                    && !string.IsNullOrEmpty(eventArgs.LogEvent.Exception)
                    && NetworkExceptionMatchRegEx.IsMatch(eventArgs.LogEvent.Exception))
            {
               if(eventArgs.LogEvent.AssociatedNodes != null && eventArgs.LogEvent.AssociatedNodes.HasAtLeastOneElement())
                {
                    foreach(var node in eventArgs.LogEvent.AssociatedNodes)
                    {
                        var nodeState = new NodeStateChange(NodeStateChange.DetectedStates.NetworkEvent,
                                                                eventArgs.LogEvent.EventTime,
                                                                eventArgs.LogEvent.EventTimeLocal,
                                                                eventArgs.LogEvent.Node);
                        node.AssociateItem(nodeState);
                    }
                }
                else
                {
                    var nodeState = new NodeStateChange(NodeStateChange.DetectedStates.NetworkEvent,
                                                                eventArgs.LogEvent.EventTime,
                                                                eventArgs.LogEvent.EventTimeLocal);
                    eventArgs.LogEvent.Node.AssociateItem(nodeState);
                }

            }
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
			            if(this.EventParser != null && !this.EventParser.Disposed)
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
            if(propTable.TryGetValue(key, out object value))
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
            if (propTable.TryGetValue(key, out object value))
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
            if (propTable.TryGetValue(key, out object value) && value.IsNumber())
            {
                return (long)((dynamic)value);                
            }
            return 0L;
        }

        public static decimal GetPropDecimalValue(this IReadOnlyDictionary<string, object> propTable, string key)
        {           
            if (propTable.TryGetValue(key, out object value) && value.IsNumber())
            {
                return (decimal)((dynamic)value);               
            }
            return 0M;
        }

        public static decimal GetPropUOMValue(this IReadOnlyDictionary<string, object> propTable, string key, UnitOfMeasure.Types uomConvertType = UnitOfMeasure.Types.Unknown)
        {            
            if (propTable.TryGetValue(key, out object value))
            {
                if(value != null)
                {
                    if(value is UnitOfMeasure uom)
                    {                        
                        if (uomConvertType == UnitOfMeasure.Types.Unknown) return uom.UnitType == UnitOfMeasure.Types.Percent ? uom.Value / 100M : uom.Value;

                        return uom.ConvertTo(uomConvertType);                        
                    }
                    else if(value.IsNumber())
                    {
                        return (decimal)((dynamic)value);
                    }
                }
            }

            return -1M;
        }

        public static UnitOfMeasure? GetPropUOM(this IReadOnlyDictionary<string, object> propTable, string key)
        {
            if (propTable.TryGetValue(key, out object value))
            {
                if (value != null)
                {
                    if (value is UnitOfMeasure uom)
                    {                        
                        return uom;
                    }                    
                }
            }

            return (UnitOfMeasure?) null;
        }

        public static long IsOffSetValue(this IReadOnlyDictionary<string, object> propTable, string key, ref long offsetValue)
        {           
            if (propTable.TryGetValue(key, out object value) && value.IsNumber())
            {
                long num = (long)((dynamic)value);

                if (num >= offsetValue) return num - offsetValue;

                offsetValue = 0;
                return num;
            }

            return -1;
        }

        public static LogEventGrouping StatusLoggerPoolStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {            
            var poolPendingItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbrpending")).Where(i => i > 0L);
            var poolCompletedItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbrcompleted")).Where(i => i > 0);
            var poolBlockedItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbrblocked")).Where(i => i > 0);
            var poolBlockedAllTimeItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("nbralltimeblocked")).Where(i => i > 0);
                       
            return poolBlockedAllTimeItems.IsEmpty() && poolBlockedItems.IsEmpty() && poolCompletedItems.IsEmpty() && poolPendingItems.IsEmpty() 
                    ? null
                    : new LogEventGrouping(ref logEventGroup,
                                            assocatedLogEvents,
                                            LogEventGrouping.GroupingTypes.StatusLoggerPoolStats,
                                            poolPendingItems,
                                            poolCompletedItems,
                                            poolBlockedItems,
                                            poolBlockedAllTimeItems);
        }

        public static LogEventGrouping StatusLoggerMemTblOPSStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {            
            var poolOPSItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositive("memtable")).Where(i => i > 0L);
            var poolSizeItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("datasize", UnitOfMeasure.Types.MiB)).Where(i => i > 0L);
            
            return poolOPSItems.IsEmpty() && poolSizeItems.IsEmpty()
                    ? null
                    :new LogEventGrouping(ref logEventGroup,
                                            assocatedLogEvents,
                                            LogEventGrouping.GroupingTypes.StatusLoggerMemTblOPSStats,
                                            new IEnumerable<long>[] { poolOPSItems },
                                            new IEnumerable<decimal>[] { poolSizeItems });
        }

        public static LogEventGrouping LargePartitionRowStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var partitionSizeItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("size", UnitOfMeasure.Types.MiB)).Where(i => i >= 0M);
           
            return partitionSizeItems.IsEmpty()
                    ? null
                    : new LogEventGrouping(ref logEventGroup,
                                            assocatedLogEvents,
                                            LogEventGrouping.GroupingTypes.LargePartitionRowStats,
                                            partitionSizeItems);
        }

        public static LogEventGrouping TombstoneStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var tombstoneItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositiveZero(Properties.StatPropertyNames.Default.LogTombstones)).Where(i => i >= 0L);
            var liveItems = assocatedLogEvents.Select(i => i.LogProperties.IsPositiveZero(Properties.StatPropertyNames.Default.LogLiveCells)).Where(i => i >= 0L);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.TombstoneStats,
                                        liveItems,
                                        tombstoneItems);
        }

        public static LogEventGrouping GCStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {            
            var poolEdenFromItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("edenspacefrom", UnitOfMeasure.Types.MiB)).Where(i => i >= 0M);
            var poolEdenToItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("edenspaceto", UnitOfMeasure.Types.MiB)).Where(i => i >= 0M);
            var poolOldGenFromItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("oldgenfrom", UnitOfMeasure.Types.MiB)).Where(i => i >= 0M);
            var poolOldGenToItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("oldgento", UnitOfMeasure.Types.MiB)).Where(i => i >= 0M);
            var poolSurvivorFromItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("survivorspacefrom", UnitOfMeasure.Types.MiB)).Where(i => i >= 0M);
            var poolSurvivorToItems = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("survivorspaceto", UnitOfMeasure.Types.MiB)).Where(i => i >= 0M);
            var occurrenceCounts = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("occurrences")).Where(i => i >= 0L);
            IEnumerable<long> occurrenceDuration = null;

            if(occurrenceCounts.HasAtLeastOneElement())
            {
                var occurrenceMax = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("durationmax")).Where(i => i >= 0L);
                var occurrenceMin = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("durationmin")).Where(i => i >= 0L);

                occurrenceDuration = occurrenceMax.Concat(occurrenceMin);
            }

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.GCStats,
                                        new IEnumerable<long>[]
                                        {
                                            occurrenceCounts,
                                            occurrenceDuration
                                        },
                                        new IEnumerable<decimal>[]
                                        {
                                            poolEdenFromItems,
                                            poolEdenToItems,
                                            poolOldGenFromItems,
                                            poolOldGenToItems,
                                            poolSurvivorFromItems,
                                            poolSurvivorToItems
                                        });
        }

        public static LogEventGrouping GossipPendingStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var pending = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("pendingtasks")).Where(i => i >= 0M);
            
            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.GossipPendingStats,
                                        pending);
        }

        public static LogEventGrouping BatchStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var nbrPartitions = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("nbrpartitions")).Where(i => i > 0);
            var sizes = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("size", UnitOfMeasure.Types.MiB)).Where(i => i > 0M);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.BatchStats,
                                        new IEnumerable<long>[] { nbrPartitions },
                                        new IEnumerable<decimal>[] { sizes });
        }

        public static LogEventGrouping PreparesDiscardedStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var nbrPrepares = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("prepares")).Where(i => i > 0);
            var sizes = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("cache", UnitOfMeasure.Types.MiB));

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.PreparesDiscard,
                                        new IEnumerable<long>[] { nbrPrepares },
                                        new IEnumerable<decimal>[] { sizes });
        }

        public static LogEventGrouping GCPauseStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var pauseDurations = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("pause", UnitOfMeasure.Types.MS));

            var durationStats = new LogEventGrouping.DurationStatItems(pauseDurations);
            return durationStats.HasValue ? new LogEventGrouping(ref logEventGroup, durationStats) : null;
        }

        public static LogEventGrouping FlushStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var ops = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("operationspersec")).Where(i => i > 0);
            var serializedHeaps = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("serializedheap", UnitOfMeasure.Types.MiB)).Where(i => i > 0);
            var flushedStorage = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("flushedstorage", UnitOfMeasure.Types.MiB)).Where(i => i > 0);
            var flushSize = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("onheap", UnitOfMeasure.Types.MiB)).Where(i => i > 0);
            IEnumerable<decimal> flushThreshold = Enumerable.Empty<decimal>();
            
            if(flushSize.HasAtLeastOneElement())
            {
                flushThreshold = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("onheapthresholdpercent")).Where(i => i >= 0);
            }
            else
            {
                flushSize = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("offheap", UnitOfMeasure.Types.MiB)).Where(i => i > 0);
                if (flushSize.HasAtLeastOneElement())
                {
                    flushThreshold = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("offheapthresholdpercent")).Where(i => i >= 0);
                }
            }


            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.FlushStats,
                                        new IEnumerable<long>[] { ops },
                                        new IEnumerable<decimal>[] { flushSize, flushThreshold, serializedHeaps, flushedStorage,  Enumerable.Empty<decimal>() });
        }

        public static LogEventGrouping CompactionStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var size = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("size", UnitOfMeasure.Types.MiB)).Where(i => i >= 0);
            var newSize = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("newsize", UnitOfMeasure.Types.MiB)).Where(i => i >= 0);
            var readRate = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("readrate", UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.SEC)).Where(i => i > 0);
            var iorate = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("iorate", UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.SEC)).Where(i => i >= 0);
            var rowrate = assocatedLogEvents.Select(i => (long) i.LogProperties.GetPropUOMValue("rowsrate")).Where(i => i >= 0);
            var mergedCnt = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("mergedpartitions")).Where(i => i >= 0);
            var newMergeCnt = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("mergecounts")).Where(i => i >= 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.CompactionStats,
                                        new IEnumerable<long>[] { rowrate, mergedCnt, newMergeCnt },
                                        new IEnumerable<decimal>[] { size, newSize, readRate, iorate });
        }

        public static LogEventGrouping HintHandOffStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var rows = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("rows")).Where(i => i > 0);

            return rows.IsEmpty()
                        ? null
                        : new LogEventGrouping(ref logEventGroup,
                                                assocatedLogEvents,
                                                LogEventGrouping.GroupingTypes.HintHandOffStats,
                                                rows);
        }

        public static LogEventGrouping MaximumMemoryUsageReachedStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var maxSize = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("max", UnitOfMeasure.Types.MiB)).Where(i => i >= 0);
            var chuckSize = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("chuck", UnitOfMeasure.Types.MiB)).Where(i => i >= 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.MaximumMemoryUsageReachedStats,
                                        maxSize, chuckSize);
        }

        public static LogEventGrouping FreeDeviceStorageStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var storage = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("free", UnitOfMeasure.Types.MiB)).Where(i => i >= 0);
            
            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.FreeDeviceStorageStats,
                                        storage);
        }

        public static LogEventGrouping CompactionInsufficientSpaceStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var storage = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue(Properties.StatPropertyNames.Default.LogInsufficientSpace, UnitOfMeasure.Types.MiB)).Where(i => i >= 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.CompactionInsufficientSpaceStats,
                                        storage);
        }

        public static LogEventGrouping DropStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var drpInternal = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("drpinteral")).Where(i => i > 0);
            var drpCrossNode = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("drpcrossnode")).Where(i => i > 0);
            var drpLatInternal = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("drpmeanlatencyinteral", UnitOfMeasure.Types.MS)).Where(i => i > 0);
            var drpLatCrossNode = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("drpmeanlatencycrossnode", UnitOfMeasure.Types.MS)).Where(i => i > 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.DropStats,
                                        new IEnumerable<long>[] { drpInternal, drpCrossNode },
                                        new IEnumerable<decimal>[] { drpLatInternal, drpLatCrossNode });
        }

        public static LogEventGrouping SolrExpiredStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var fndRows = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("fndrows")).Where(i => i > 0);
            var expiredDocs = assocatedLogEvents.Select(i => i.LogProperties.GetPropLongValue("expireddocs")).Where(i => i > 0);
            var fndRowDurations = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("fndrowsduration", UnitOfMeasure.Types.MS)).Where(i => i > 0);
            var expiredDocDurations = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("expireddocsduration", UnitOfMeasure.Types.MS)).Where(i => i > 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.SolrExpiredStats,
                                        new IEnumerable<long>[] { fndRows, expiredDocs },
                                        new IEnumerable<decimal>[] { fndRowDurations, expiredDocDurations });
        }

        public static LogEventGrouping NodeSyncStats(ref LogEventGroup logEventGroup, string analyticsGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {
            var sizes = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("size", UnitOfMeasure.Types.MiB));
            var rates = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("rate", UnitOfMeasure.Types.Rate | UnitOfMeasure.Types.SEC | UnitOfMeasure.Types.MiB));
            var inconstPerct = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("inconst"));
            var partialPerct = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("partial")).Where(i => i > 0);
            var failedPerct = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("failed")).Where(i => i > 0);
            var uncompletedPerct = assocatedLogEvents.Select(i => i.LogProperties.GetPropUOMValue("uncompleted")).Where(i => i > 0);

            return new LogEventGrouping(ref logEventGroup,
                                        assocatedLogEvents,
                                        LogEventGrouping.GroupingTypes.NodeSyncStats,
                                        sizes,
                                        rates,
                                        inconstPerct,
                                        partialPerct,
                                        failedPerct,
                                        uncompletedPerct);
        }

    }

    public struct LogEventGroup
    {
        
        public LogEventGroup(DateTimeRange aggregationDateTime, ILogEvent logEvent)
            : this()
        {            
            this.AggregationDateTime = aggregationDateTime.Min;
            this.DataCenterName = logEvent.DataCenter?.Name;
            this.NodeName = logEvent.Node?.NodeName();
            this.KeySpaceName = logEvent.Keyspace?.Name;

            if (logEvent.DDLItems == null || logEvent.DDLItems.IsEmpty())
            {
                this.TableName = logEvent.TableViewIndex?.NameWAttrs();
            }
            else
            {
                this.TableName = string.Join(",", logEvent.DDLItems.Select(i => i.NameWAttrs(true)).Sort());
            }
            {
                var logClass = (logEvent.Class & ~EventClasses.LogTypes & ~EventClasses.Orphaned);
                string assocItem = null;
                var subClass = logEvent.SubClass;
                
                if (string.IsNullOrEmpty(subClass))
                {
                    this.Class = logClass.ToString();
                }
                else
                {
                    this.Class = logClass.ToString() + '(' + subClass.Trim() + ')';
                }


                if (logEvent.AssociatedNodes != null && logEvent.AssociatedNodes.HasAtLeastOneElement())
                {
                    if (assocItem == null)
                        assocItem = string.Empty;
                    else
                        assocItem += ", ";
                    assocItem += string.Join(",", logEvent.AssociatedNodes.Select(n => n.DataCenter.Name + '|' + n.Id.NodeName()));
                }                
                if (logEvent.TokenRanges != null && logEvent.TokenRanges.HasAtLeastOneElement())
                {
                    if (assocItem == null)
                        assocItem = string.Empty;
                    else
                        assocItem += ", ";
                    assocItem += "Tokens{ " + string.Join(",", logEvent.TokenRanges.Select(n => n?.ToString()).Where(s => s != null)) + "}";
                }
                if (logEvent.LogProperties.ContainsKey("partitionkey"))
                {
                    if (assocItem == null)
                        assocItem = string.Empty;
                    else
                        assocItem += ", ";
                    assocItem += "PartitionKey{ " + (logEvent.LogProperties["partitionkey"]?.ToString() ?? string.Empty) + "}";
                }
                if (logEvent.LogProperties.ContainsKey("whereclause"))
                {
                    if (assocItem == null)
                        assocItem = string.Empty;
                    else
                        assocItem += ", ";
                    assocItem += "WhereClause{ " + (logEvent.LogProperties["whereclause"]?.ToString() ?? string.Empty) + "}";
                }
                if (logEvent.LogProperties.ContainsKey("tag"))
                {
                    if (assocItem == null)
                        assocItem = string.Empty;
                    else
                        assocItem += ", ";
                    assocItem += "Tag{ " + (logEvent.LogProperties["tag"]?.ToString() ?? string.Empty) + "}";
                }
                if (logEvent.LogProperties.ContainsKey("tag1"))
                {
                    var pathFld = this.Path;

                    if (pathFld == null)
                        pathFld = string.Empty;
                    else
                        pathFld += "; ";

                    this.Path = pathFld + "Tag{ " + (logEvent.LogProperties["tag1"]?.ToString() ?? string.Empty) + "}";
                }
                if (logEvent.LogProperties.ContainsKey("options"))
                {
                    var pathFld = this.Path;

                    if (pathFld == null)
                        pathFld = string.Empty;
                    else
                        pathFld += "; ";

                    var options = logEvent.LogProperties["options"];

                    if(options != null && options is IEnumerable<object>)
                    {
                        this.Path = pathFld + "options{ "
                                                + string.Join("; ", ((IEnumerable<object>)options)
                                                                        .Select(i => i?.ToString() ?? string.Empty))
                                                + "}";
                    }
                    else
                        this.Path = pathFld + "options{ " + (options?.ToString() ?? string.Empty) + "}";
                }
                if (logEvent.LogProperties.ContainsKey("nodetxt"))
                {                    
                    var unknownNode = logEvent.LogProperties["nodetxt"];

                    if (unknownNode != null)
                    {
                        var unknownNodes = (unknownNode is string
                                                ? ((string)unknownNode).Split(',')
                                                : (unknownNode is IEnumerable<object>
                                                        ? ((IEnumerable<object>)unknownNode).Cast<string>().ToArray()
                                                        : new string[] { unknownNode.ToString() }))
                                        .Select(u => u?.Trim())
                                        .Where(u => !string.IsNullOrEmpty(u) && logEvent.Cluster?.TryGetNode(u) == null)
                                        .Select(u => u[0] == '/' ? u.Substring(1) : u);

                        if (unknownNodes.HasAtLeastOneElement())
                        {
                            if (assocItem == null)
                                assocItem = string.Empty;
                            else
                                assocItem += ", ";

                            assocItem += "UnknownNode{ " + string.Join(", ", unknownNodes.DuplicatesRemoved(u => u)) + "}";
                        }
                    }             
                }

                if (!string.IsNullOrEmpty(assocItem))
                {
                    this.AssocItem = assocItem;
                }
            }

            //this.Orphaned = (logEvent.Class & EventClasses.Orphaned) == EventClasses.Orphaned;
            if((logEvent.Type & EventTypes.ExceptionElement) == EventTypes.ExceptionElement)
            {
                if(logEvent.LogProperties.TryGetValue("error", out object errValue)
                        && errValue != null)
                {
                    string errorStr;

                    if (errValue is IEnumerable<object>)
                        errorStr = string.Join("; ", ((IEnumerable<object>)errValue).Select(i => i.ToString()));
                    else
                        errorStr = errValue.ToString();

                    if (!string.IsNullOrEmpty(errorStr))
                    {
                        if (string.IsNullOrEmpty(logEvent.Exception))
                            this.Exception = errorStr;
                        else
                            this.Exception = errorStr + "; " + logEvent.Exception;
                    }
                }
                this.Class = "Exception, " + this.Class;
            }
            else
                this.Exception = logEvent.Exception;

            if(this.Path == null)
                this.Path = logEvent.ExceptionPath?.Join("=>", s => s);
            else if(logEvent.ExceptionPath != null && logEvent.ExceptionPath.HasAtLeastOneElement())
                this.Path += ';' + logEvent.ExceptionPath.Join("=>", s => s);
        }

        public readonly DateTime AggregationDateTime;
        public readonly string DataCenterName;
        public readonly string NodeName;
        public readonly string KeySpaceName;
        public readonly string TableName;
        public readonly string Class;
        public readonly string AssocItem;
        //public readonly bool Orphaned;
        public readonly string Exception;
        public readonly string Path;                                    
    }

    public sealed class LogEventGrouping
    {

        public enum GroupingTypes
        {
            DurationStats = 0,
            StatusLoggerPoolStats,
            StatusLoggerMemTblOPSStats,
            LargePartitionRowStats,
            TombstoneStats,
            GCStats,
            GossipPendingStats,
            BatchStats,
            FlushStats,
            CompactionStats,
            HintHandOffStats,
            PreparesDiscard,
            MaximumMemoryUsageReachedStats,
            FreeDeviceStorageStats,
            CompactionInsufficientSpaceStats,
            DropStats,
            SolrExpiredStats,
            NodeSyncStats
        }

        public static IEnumerable<LogEventGrouping> CreateLogEventGrouping(LogEventGroup logEventGroup, IEnumerable<ILogEvent> assocatedLogEvents)
        {            
            return assocatedLogEvents.GroupBy(i => i.AnalyticsGroup)
                        .Select(logEvtGrp =>
                        {
                            if (string.IsNullOrEmpty(logEvtGrp.Key))
                            {
                                return new LogEventGrouping(ref logEventGroup,
                                                            logEvtGrp,
                                                            LogEventGrouping.GroupingTypes.DurationStats,
                                                            null, null, true);
                            }
                            
                            if(LibrarySettings.AggregateGroups.TryGetValue(logEvtGrp.Key, out AggregateGroups aggregateGroup))
                            {
                                return aggregateGroup.AggregateGroupFunction(ref logEventGroup, logEvtGrp.Key, logEvtGrp);
                            }

                            Logger.Instance.WarnFormat("Aggregate Group \"{0}\" was not found in AggregateGroups using CassandraLogEventAggregate.DurationStats function", logEvtGrp.Key);
                            return new LogEventGrouping(ref logEventGroup,
                                                            logEvtGrp,
                                                            LogEventGrouping.GroupingTypes.DurationStats,
                                                            null, null, true);
                        })
                        .Where(i => i?.HasValue ?? false);            
        }

        /// <summary>
        /// General Events or events with no Aggregate Group
        /// 
        ///  GroupingTypes.DurationStats: longAggreations and decimalAggreations are ignored
        ///  GroupingTypes.StatusLoggerPoolStats: decimalAggreations is ignored where longAggreations is pending, completed, blocked, alltimeBlocked
        ///  GroupingTypes.StatusLoggerMemTblOPSStats: longAggreations is ops and decimalAggreations is size
        ///  GroupingTypes.LargePartitionRowStats: longAggreations is ignored and decimalAggreations is size
        ///  GroupingTypes.TombstoneStats: longAggreations are live and cells; decimalAggreations is ignored
        ///  GroupingTypes.GCStats:  decimalAggreations is beforeEden, afterEden, beforeOld, afterOld, beforeSurvivor, afterSurvivor; longAggreations is ignored
        ///  GroupingTypes.GossipPendingStats: longAggreations is pending; decimalAggreations is ignored
        /// <param name="assocatedLogEvents">
        /// </param>
        /// <param name="decimalAggreations">
        /// </param>
        /// <param name="groupKey"></param>
        /// <param name="longAggreations"></param>
        /// <param name="type"></param>
        /// <param name="forceHasValue"></param>
        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                GroupingTypes type,
                                IEnumerable<long>[] longAggreations,
                                IEnumerable<decimal>[] decimalAggreations,
                                bool forceHasValue = false)
        {
            
            switch (type)
            {
                case GroupingTypes.DurationStats:
                    this.GroupKey = groupKey;
                    this.DurationStats = new DurationStatItems(assocatedLogEvents);
                    this.HasValue = forceHasValue ? true : this.DurationStats?.HasValue ?? false;
                    break;
                case GroupingTypes.StatusLoggerPoolStats:
                    this.GroupKey = groupKey;
                    this.PoolStats = new PoolStatItems(longAggreations[0], longAggreations[1], longAggreations[2], longAggreations[3]);
                    this.HasValue = forceHasValue ? true : this.PoolStats.HasValue;
                    break;
                case GroupingTypes.StatusLoggerMemTblOPSStats:
                    this.GroupKey = groupKey;
                    this.PoolMemTblOPSStats = new PoolMemTblOPSItems(longAggreations[0], decimalAggreations[0]);
                    this.HasValue = forceHasValue ? true : this.PoolMemTblOPSStats.HasValue;
                    break;
                case GroupingTypes.LargePartitionRowStats:
                    this.GroupKey = groupKey;
                    this.LargePartitionRowStats = new LargePartitionRowItems(decimalAggreations[0]);
                    this.HasValue = forceHasValue ? true : this.LargePartitionRowStats.HasValue;
                    break;
                case GroupingTypes.TombstoneStats:
                    this.GroupKey = groupKey;
                    this.TombstoneStats = new TombstoneItems(longAggreations[0], longAggreations[1]);
                    this.HasValue = forceHasValue ? true : this.TombstoneStats.HasValue;
                    break;
                case GroupingTypes.GCStats:
                    this.GroupKey = groupKey;
                    this.DurationStats = new DurationStatItems(assocatedLogEvents);
                    this.GCStats = new GCStatItems(decimalAggreations[0],
                                                    decimalAggreations[1],
                                                    decimalAggreations[2],
                                                    decimalAggreations[3],
                                                    decimalAggreations[4],
                                                    decimalAggreations[5],
                                                    longAggreations[0],
                                                    longAggreations[1]);
                    this.HasValue = forceHasValue ? true : (this.DurationStats?.HasValue ?? false) || this.GCStats.HasValue;
                    break;
                case GroupingTypes.GossipPendingStats:
                    this.GroupKey = groupKey;
                    this.GossipPendingStats = new GossipPendingItems(longAggreations[0]);
                    this.HasValue = forceHasValue ? true : this.GossipPendingStats.HasValue;
                    break;
                case GroupingTypes.BatchStats:
                    this.GroupKey = groupKey;
                    this.BatchSizeStats = new BatchSizeItems(longAggreations[0], decimalAggreations[0]);
                    this.HasValue = forceHasValue ? true : this.BatchSizeStats.HasValue;
                    break;
                case GroupingTypes.PreparesDiscard:
                    this.GroupKey = groupKey;
                    this.PreparesDiscardedStats = new PreparesDiscardedItems(longAggreations[0], decimalAggreations[0]);
                    this.HasValue = forceHasValue ? true : this.PreparesDiscardedStats.HasValue;
                    break;
                case GroupingTypes.FlushStats:
                    this.GroupKey = groupKey;
                    this.DurationStats = new DurationStatItems(assocatedLogEvents);
                    this.FlushStats = new FlushItems(longAggreations[0],
                                                        decimalAggreations[0],
                                                        decimalAggreations[1],
                                                        decimalAggreations[2],
                                                        decimalAggreations[3],
                                                        decimalAggreations[4]);
                    this.HasValue = forceHasValue ? true : (this.DurationStats?.HasValue ?? false) || this.FlushStats.HasValue;
                    break;
                case GroupingTypes.CompactionStats:
                    this.GroupKey = groupKey;
                    this.DurationStats = new DurationStatItems(assocatedLogEvents);
                    this.CompactionStats = new CompactionItems(longAggreations[0],
                                                                decimalAggreations[0],
                                                                decimalAggreations[1],
                                                                decimalAggreations[2],
                                                                decimalAggreations[3],
                                                                longAggreations[1],
                                                                longAggreations[2]);
                    this.HasValue = forceHasValue ? true : (this.DurationStats?.HasValue ?? false) || this.CompactionStats.HasValue;
                    break;
                case GroupingTypes.HintHandOffStats:
                    this.GroupKey = groupKey;
                    this.DurationStats = new DurationStatItems(assocatedLogEvents);
                    this.HintHandOffStats = new HintHandOffItems(longAggreations[0]);
                    this.HasValue = forceHasValue ? true : (this.DurationStats?.HasValue ?? false) || this.HintHandOffStats.HasValue;
                    break;
                case GroupingTypes.MaximumMemoryUsageReachedStats:
                    this.GroupKey = groupKey;
                    this.MaximumMemoryUsageReachedStats = new MaximumMemoryUsageReached(decimalAggreations[0], decimalAggreations[1]);
                    this.HasValue = forceHasValue ? true : this.MaximumMemoryUsageReachedStats.HasValue;
                    break;
                case GroupingTypes.FreeDeviceStorageStats:
                    this.GroupKey = groupKey;
                    this.FreeDeviceStorageStats = new FreeDeviceStorage(decimalAggreations[0]);
                    this.HasValue = forceHasValue ? true : this.FreeDeviceStorageStats.HasValue;
                    break;
                case GroupingTypes.CompactionInsufficientSpaceStats:
                    this.GroupKey = groupKey;
                    this.CompactionInsufficientSpaceStats = new CompactionInsufficientSpace(decimalAggreations[0]);
                    this.HasValue = forceHasValue ? true : this.CompactionInsufficientSpaceStats.HasValue;
                    break;
                case GroupingTypes.DropStats:
                    this.GroupKey = groupKey;
                    this.DropStats = new Drops(longAggreations[0],
                                                longAggreations[1],
                                                decimalAggreations[0],
                                                decimalAggreations[1]);
                    this.HasValue = forceHasValue ? true : this.DropStats.HasValue;
                    break;
                case GroupingTypes.SolrExpiredStats:
                    this.GroupKey = groupKey;
                    this.DurationStats = new DurationStatItems(assocatedLogEvents);
                    this.SolrExpiredStats = new SolrExpired(longAggreations[0],
                                                                longAggreations[1],
                                                                decimalAggreations[0],
                                                                decimalAggreations[1]);
                    this.HasValue = forceHasValue ? true : (this.DurationStats?.HasValue ?? false) || this.SolrExpiredStats.HasValue;
                    break;
                case GroupingTypes.NodeSyncStats:
                    this.GroupKey = groupKey;
                    this.DurationStats = new DurationStatItems(assocatedLogEvents);
                    this.NodeSyncStats = new NodeSync(decimalAggreations[0],
                                                                decimalAggreations[1],
                                                                decimalAggreations[2],
                                                                decimalAggreations[3],
                                                                decimalAggreations[4],
                                                                decimalAggreations[5]);
                    this.HasValue = forceHasValue ? true : (this.DurationStats?.HasValue ?? false) || this.NodeSyncStats.HasValue;
                    break;
                default:
                    break;
            }
            
        }

        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                GroupingTypes type,
                                params IEnumerable<long>[] longAggreations)
            : this(ref groupKey, assocatedLogEvents, type, longAggreations, null)
        {
        }

        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                GroupingTypes type,                                
                                params IEnumerable<decimal>[] decimalAggreations)
            : this(ref groupKey, assocatedLogEvents, type, null, decimalAggreations)
        {
        }

        public LogEventGrouping(ref LogEventGroup groupKey,
                                IEnumerable<ILogEvent> assocatedLogEvents,
                                GroupingTypes type,
                                IEnumerable<long> longAggreation,
                                params IEnumerable<decimal>[] decimalAggreations)
            : this(ref groupKey, assocatedLogEvents, type,
                    longAggreation != null && longAggreation.HasAtLeastOneElement() ? new IEnumerable<long>[] { longAggreation } : null,
                    decimalAggreations)
        {
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
                    this.Max = (decimal) itemCollection.Max();
                    this.Min = (decimal) itemCollection.Min();
                    this.Mean = (decimal) itemCollection.Average();
                    this.StdDev = (decimal) itemCollection.StandardDeviationP();
                    this.Sum = (decimal)itemCollection.Sum();
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
                                            .Select(e => (decimal) e.Duration.Value.TotalMilliseconds);

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

            public DurationStatItems(IEnumerable<decimal> duration)
            {
                if (duration.HasAtLeastOneElement())
                {
                    this.Duration = new ItemStatsDecimal(duration);
                    this.HasValue = this.Duration.HasValue;
                }
            }

            public readonly bool HasValue;
            public readonly ItemStatsDecimal Duration;
            public readonly string UOM = "MS";
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
                                IEnumerable<decimal> beforeSurvivor, IEnumerable<decimal> afterSurvivor,
                                IEnumerable<long> gcOccurrences,
                                IEnumerable<long> gcOccurrenceDurations)               
            {               
                if(beforeEden != null && beforeEden.HasAtLeastOneElement())
                    this.Eden = new GCBeforeAfterItems(beforeEden, afterEden);
                if(beforeOld != null && beforeOld.HasAtLeastOneElement())
                    this.Old = new GCBeforeAfterItems(beforeOld, afterOld);
                if(beforeSurvivor != null && beforeSurvivor.HasAtLeastOneElement())
                    this.Survivor = new GCBeforeAfterItems(beforeSurvivor, afterSurvivor);
                if (gcOccurrences != null && gcOccurrences.HasAtLeastOneElement())
                    this.GCOccurrences = new ItemStatsLong(gcOccurrences);

                if (gcOccurrenceDurations != null && gcOccurrenceDurations.HasAtLeastOneElement())
                    this.GCOccurrenceDuration = new ItemStatsLong(gcOccurrenceDurations);
                
                if ((this.Eden != null && this.Eden.HasValue)
                        | (this.Old != null && this.Old.HasValue)
                        | (this.Survivor != null && this.Survivor.HasValue))
                {
                    this.UOM = "MiB";
                    this.HasValue = true;
                    if (this.GCOccurrences.HasValue)
                    {
                        this.UOM += ", Occurrences";
                        if (this.GCOccurrenceDuration.HasValue)
                        {
                            this.UOM += ", MS";
                        }
                    }
                }
                else if (this.GCOccurrences.HasValue)
                {
                    this.HasValue = true;
                    this.UOM = "Occurrences";
                    if (this.GCOccurrenceDuration.HasValue)
                    {
                        this.UOM += ", MS";
                    }
                }                
            }

            public readonly bool HasValue;            
            public readonly GCBeforeAfterItems Eden;
            public readonly GCBeforeAfterItems Old;
            public readonly GCBeforeAfterItems Survivor;
            public readonly ItemStatsLong GCOccurrences;
            public readonly ItemStatsLong GCOccurrenceDuration;
            public readonly string UOM;
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

        public sealed class GossipPendingItems
        {
            public GossipPendingItems(IEnumerable<long> pending)
            {
                this.Pending = new ItemStatsLong(pending);
                
                this.HasValue = this.Pending.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong Pending;            
            public readonly string UOM = "Items";
        }

        public readonly GossipPendingItems GossipPendingStats;

        public sealed class BatchSizeItems
        {
            public BatchSizeItems(IEnumerable<long> nbrPartitions, IEnumerable<decimal> batchSized)
            {
                this.NbrPartitions = new ItemStatsLong(nbrPartitions);
                if (this.NbrPartitions.HasValue)
                    this.UOM = "Partitions";                

                this.BatchSizes = new ItemStatsDecimal(batchSized);
                if (this.BatchSizes.HasValue)
                    if (this.UOM == null) this.UOM = "MIB"; else this.UOM += ", MiB";

                this.HasValue = this.NbrPartitions.HasValue || this.BatchSizes.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong NbrPartitions;
            public readonly ItemStatsDecimal BatchSizes;
            public readonly string UOM;
        }

        public readonly BatchSizeItems BatchSizeStats;

        public sealed class FlushItems
        {
            public FlushItems(IEnumerable<long> ops,
                                IEnumerable<decimal> flushSizes,
                                IEnumerable<decimal> flushthresholds,
                                IEnumerable<decimal> serializedSizes,
                                IEnumerable<decimal> storageSizes,
                                IEnumerable<decimal> relatedIORates)
            {
                this.OPS = new ItemStatsLong(ops);
                if (this.OPS.HasValue) this.UOM = "Operation/Sec";
                this.FlushSize = new ItemStatsDecimal(flushSizes);                
                this.FlushThreshold = new ItemStatsDecimal(flushthresholds);
                if (this.FlushThreshold.HasValue)
                    if (this.UOM == null) this.UOM = "MCT"; else this.UOM += ", MCT";
                this.SerializedSize = new ItemStatsDecimal(serializedSizes);                
                this.StorageSize = new ItemStatsDecimal(storageSizes);                
                this.RelatedIORate = new ItemStatsDecimal(relatedIORates);
                if (this.RelatedIORate.HasValue)
                    if (this.UOM == null) this.UOM = "MiB/Sec"; else this.UOM += ", MiB/Sec";

                if (this.StorageSize.HasValue || this.SerializedSize.HasValue || this.FlushSize.HasValue)
                    if (this.UOM == null) this.UOM = "MiB"; else this.UOM += ", MiB";
                this.HasValue = this.OPS.HasValue || this.FlushThreshold.HasValue || this.RelatedIORate.HasValue || this.StorageSize.HasValue || this.SerializedSize.HasValue || this.FlushSize.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong OPS;
            public readonly ItemStatsDecimal FlushSize;
            public readonly ItemStatsDecimal FlushThreshold;
            public readonly ItemStatsDecimal SerializedSize;
            public readonly ItemStatsDecimal StorageSize;
            public readonly ItemStatsDecimal RelatedIORate;
            public readonly string UOM;
        }

        public readonly FlushItems FlushStats;

        public sealed class CompactionItems
        {
            public CompactionItems(IEnumerable<long> rowOPS,
                                    IEnumerable<decimal> oldSizes,
                                    IEnumerable<decimal> newSize,
                                    IEnumerable<decimal> readRate,
                                    IEnumerable<decimal> writeRate,
                                    IEnumerable<long> partMergedCnt,
                                    IEnumerable<long> partNewMergedCnt)
            {
                this.RowOPS = new ItemStatsLong(rowOPS);
                if (this.RowOPS.HasValue) this.UOM = "Row/Sec";

                this.OldSize = new ItemStatsDecimal(oldSizes);
                this.NewSize = new ItemStatsDecimal(newSize);

                this.ReadRate = new ItemStatsDecimal(readRate);
                if (this.ReadRate.HasValue)
                    if (this.UOM == null) this.UOM = "Read MiB/Sec"; else this.UOM += ", Read MiB/Sec";

                this.WriteRate = new ItemStatsDecimal(writeRate);
                if (this.WriteRate.HasValue)
                    if (this.UOM == null) this.UOM = "Write MiB/Sec"; else this.UOM += ", Write MiB/Sec";

                var maxLmt = Math.Min(partMergedCnt.Count(), partNewMergedCnt.Count());
                var diffCnt = new List<long>(maxLmt);

                for(int nIdx = 0; nIdx < maxLmt; ++ nIdx)
                {
                    diffCnt.Add(partNewMergedCnt.ElementAt(nIdx) - partMergedCnt.ElementAt(nIdx));
                }

                this.PartitionDifferenceMergeCnt = new ItemStatsLong(diffCnt);
                if (this.PartitionDifferenceMergeCnt.HasValue)
                    if (this.UOM == null) this.UOM = "Merge Cnt diff"; else this.UOM += ", Merge Cnt diff";

                this.HasValue = this.RowOPS.HasValue || this.OldSize.HasValue || this.NewSize.HasValue || this.ReadRate.HasValue || this.WriteRate.HasValue || this.PartitionDifferenceMergeCnt.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong RowOPS;
            public readonly ItemStatsDecimal OldSize;
            public readonly ItemStatsDecimal NewSize;
            public readonly ItemStatsDecimal ReadRate;
            public readonly ItemStatsDecimal WriteRate;
            public readonly ItemStatsLong PartitionDifferenceMergeCnt;
            public readonly string UOM;
        }

        public readonly CompactionItems CompactionStats;

        public sealed class HintHandOffItems
        {
            public HintHandOffItems(IEnumerable<long> hintRows)
            {
                this.HintRows = new ItemStatsLong(hintRows);
                if (this.HintRows.HasValue) this.UOM = "Rows";

               
                this.HasValue = this.HintRows.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong HintRows;
            public readonly string UOM;
        }

        public readonly HintHandOffItems HintHandOffStats;

        public sealed class PreparesDiscardedItems
        {
            public PreparesDiscardedItems(IEnumerable<long> nbrPrepares, IEnumerable<decimal> cacheSized)
            {
                this.NbrPrepares = new ItemStatsLong(nbrPrepares);
                if (this.NbrPrepares.HasValue)
                    this.UOM = "Prepares";

                this.CacheSizes = new ItemStatsDecimal(cacheSized);
                if (this.CacheSizes.HasValue)
                    if (this.UOM == null) this.UOM = "MIB"; else this.UOM += ", MiB";

                this.HasValue = this.NbrPrepares.HasValue || this.CacheSizes.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong NbrPrepares;
            public readonly ItemStatsDecimal CacheSizes;
            public readonly string UOM;
        }

        public readonly PreparesDiscardedItems PreparesDiscardedStats;

        public sealed class MaximumMemoryUsageReached 
        {
            public MaximumMemoryUsageReached(IEnumerable<decimal> maxMemory, IEnumerable<decimal> chuckSize)
            {
                this.MaxMemory = new ItemStatsDecimal(maxMemory);
                if (this.MaxMemory.HasValue)
                    this.UOM = "MiB";

                this.ChuckMemory = new ItemStatsDecimal(chuckSize);
                if (this.ChuckMemory.HasValue)
                    if (this.UOM == null) this.UOM = "MIB";

                this.HasValue = this.MaxMemory.HasValue || this.ChuckMemory.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsDecimal MaxMemory;
            public readonly ItemStatsDecimal ChuckMemory;
            public readonly string UOM;
        }

        public readonly MaximumMemoryUsageReached MaximumMemoryUsageReachedStats;

        public sealed class FreeDeviceStorage
        {
            public FreeDeviceStorage(IEnumerable<decimal> storage)
            {
                this.FreeStorage = new ItemStatsDecimal(storage);
                if (this.FreeStorage.HasValue)
                    this.UOM = "MiB";

                this.HasValue = this.FreeStorage.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsDecimal FreeStorage;           
            public readonly string UOM;
        }

        public readonly FreeDeviceStorage FreeDeviceStorageStats;

        public sealed class CompactionInsufficientSpace
        {
            public CompactionInsufficientSpace(IEnumerable<decimal> storage)
            {
                this.RemainingStorage = new ItemStatsDecimal(storage);
                if (this.RemainingStorage.HasValue)
                    this.UOM = "MiB";

                this.HasValue = this.RemainingStorage.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsDecimal RemainingStorage;
            public readonly string UOM;
        }

        public readonly CompactionInsufficientSpace CompactionInsufficientSpaceStats;

        public sealed class Drops
        {
            public Drops(IEnumerable<long> internalDrops, IEnumerable<long> crossnodeDrops, IEnumerable<decimal> intenralLatencies, IEnumerable<decimal> crossnodeLatencies)
            {
                this.InternalDrops = new ItemStatsLong(internalDrops.Concat(crossnodeDrops));
                //this.CrossNodeDrops = new ItemStatsLong(crossnodeDrops);
                this.InternalMeanLatencies = new ItemStatsDecimal(intenralLatencies.Concat(crossnodeLatencies));
                //this.CrossNodeMeanLatencies = new ItemStatsDecimal(crossnodeLatencies);

                if (this.InternalDrops.HasValue) // || this.CrossNodeDrops.HasValue)
                    this.UOM = "Dropped Messages";
                if (this.InternalMeanLatencies.HasValue) // || this.CrossNodeMeanLatencies.HasValue)
                    this.UOM = string.IsNullOrEmpty(this.UOM) ? "MS" : this.UOM + ", MS";

                this.HasValue = this.InternalDrops.HasValue || /*this.CrossNodeDrops.HasValue ||*/ this.InternalMeanLatencies.HasValue /*|| this.CrossNodeMeanLatencies.HasValue*/;
            }

            public readonly bool HasValue;
            public readonly ItemStatsLong InternalDrops;
            //public readonly ItemStatsLong CrossNodeDrops;
            public readonly ItemStatsDecimal InternalMeanLatencies;
            //public readonly ItemStatsDecimal CrossNodeMeanLatencies;
            public readonly string UOM;
        }

        public readonly Drops DropStats;

        public sealed class SolrExpired
        {
            public SolrExpired(IEnumerable<long> rowsFnd,
                                IEnumerable<long> expiredDocs,
                                IEnumerable<decimal> rowsFndDuration,
                                IEnumerable<decimal> expiredDocsDuration)
            {
                var fndItems = rowsFnd == null ? expiredDocs : rowsFnd.Concat(expiredDocs);
                var durationItems = rowsFndDuration == null ? rowsFndDuration : rowsFndDuration.Concat(expiredDocsDuration);

                this.NbrFndExpiredDocs = new ItemStatsLong(fndItems);
                this.FndExpiredDocsDuration = new ItemStatsDecimal(durationItems);

                if(this.NbrFndExpiredDocs.HasValue && this.FndExpiredDocsDuration.HasValue)
                {
                    this.FndExpiredOPS = new ItemStatsDecimal(fndItems.Select(durationItems, (fnd, dur) => dur == 0m ? 0m : (decimal)fnd / (dur / 1000m)));
                }
                    
                if (this.NbrFndExpiredDocs.HasValue)
                    this.UOM = "Found&Expired";
                if (this.FndExpiredOPS.HasValue)
                    this.UOM = string.IsNullOrEmpty(this.UOM) ? "Found&Expired/sec" : this.UOM + ", Found&Expired/sec";
                
                this.HasValue = this.FndExpiredDocsDuration.HasValue || this.NbrFndExpiredDocs.HasValue;
            }

            public readonly bool HasValue;            
            public readonly ItemStatsLong NbrFndExpiredDocs;
            public readonly ItemStatsDecimal FndExpiredDocsDuration;
            public readonly ItemStatsDecimal FndExpiredOPS;

            public readonly string UOM;
        }

        public readonly SolrExpired SolrExpiredStats;

        public sealed class NodeSync
        {
            public NodeSync(IEnumerable<decimal> sizes,
                                IEnumerable<decimal> rates,
                                IEnumerable<decimal> percentInconsistent,
                                IEnumerable<decimal> percentPartial,
                                IEnumerable<decimal> percentFailed,
                                IEnumerable<decimal> percentUnCompleted)
            {
                this.Size = new ItemStatsDecimal(sizes);
                this.Rate = new ItemStatsDecimal(rates);
                this.PercentInconsistent = new ItemStatsDecimal(percentInconsistent);
                this.PercentPartial = new ItemStatsDecimal(percentPartial);
                this.PercentFailed = new ItemStatsDecimal(percentFailed);
                this.PercentUnCompleted = new ItemStatsDecimal(percentUnCompleted);

                if (this.Size.HasValue)
                    this.UOM = "MiB";
                if (this.Rate.HasValue)
                    this.UOM = string.IsNullOrEmpty(this.UOM) ? "mb/sec" : this.UOM + ", mb/sec";
                if (this.PercentInconsistent.HasValue)
                    this.UOM = string.IsNullOrEmpty(this.UOM) ? "%Inconsistent" : this.UOM + ", %Inconsistent";
                
                this.HasValue = this.Size.HasValue || this.Rate.HasValue || this.PercentInconsistent.HasValue;
            }

            public readonly bool HasValue;
            public readonly ItemStatsDecimal Size;
            public readonly ItemStatsDecimal Rate;
            public readonly ItemStatsDecimal PercentInconsistent;
            public readonly ItemStatsDecimal PercentPartial;
            public readonly ItemStatsDecimal PercentFailed;
            public readonly ItemStatsDecimal PercentUnCompleted;

            public readonly string UOM;
        }

        public readonly NodeSync NodeSyncStats;
    }
}
