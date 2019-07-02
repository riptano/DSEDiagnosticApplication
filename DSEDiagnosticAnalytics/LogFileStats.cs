using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Newtonsoft.Json;
using Common;

namespace DSEDiagnosticAnalytics
{
    public sealed class LogFileStats : AnalyzeData
    {
        static readonly IEnumerable<int> EmptyReconciliationRefs = Enumerable.Empty<int>();

        public LogFileStats(Cluster cluster, CancellationToken cancellationToken)
            : base(cluster, cancellationToken)
        {

        }

        #region Stats

        /// <summary>
        /// An entry represents a set of log file information including log start/end timestamps
        /// </summary>
        public sealed class LogInfoStat : DSEDiagnosticLibrary.IAggregatedStats
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

            public LogInfoStat(INode node,
                                DateTimeOffsetRange logFileRange,
                                DateTimeOffsetRange logRange,
                                IFilePath logFilePath,
                                UnitOfMeasure logFileSize,
                                OverLappingTypes overlappingType,
                                bool isDebugFile,
                                long logItems,                               
                                TimeSpan? gapLogDuration = null,
                                bool? nodeRestart = null)
            {
                this.Node = node;
                this.Path = logFilePath;
                this.LogFileRange = logFileRange;
                this.LogRange = logRange;
                this.LogFileSize = new UnitOfMeasure(logFileSize);
                this.OverlappingType = overlappingType;
                this.IsDebugFile = isDebugFile;
                this.Gap = gapLogDuration;
                this.LogItems = logItems;
                this.NodeRestart = nodeRestart;
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
            public DateTimeOffset EventTime { get { return this.LogRange.Min; } }
            public DateTime EventTimeLocal { get; }
            /// <summary>
            /// Log&apos;s starting time
            /// </summary>
            public DateTimeOffset? EventTimeBegin { get { return this.LogRange.Min; } }
            /// <summary>
            /// Log&apos;s ending time
            /// </summary>
            public DateTimeOffset? EventTimeEnd { get { return this.LogRange.Max; } }
            /// <summary>
            /// Log&apos;s duration
            /// </summary>
            public TimeSpan? Duration { get { return this.LogRange.TimeSpan(); } }

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

            public IAggregatedStats UpdateAssociateItem(string key, object value)
            {
                throw new NotImplementedException();
            }

            [JsonIgnore]
            public IEnumerable<int> ReconciliationRefs { get; } = EmptyReconciliationRefs;

            public IAggregatedStats AssociateItem(int reference)
            {
                throw new NotImplementedException();
            }
            #endregion

            public DateTimeOffsetRange LogFileRange { get; }
            public DateTimeOffsetRange LogRange { get; }
            public UnitOfMeasure LogFileSize { get; }
            public bool IsDebugFile { get; }
            /// <summary>
            /// This entry overlaps or is a duplicated to the prior entry.
            /// </summary>
            public OverLappingTypes OverlappingType { get; }
            /// <summary>
            /// This is the gap between this entry and the prior entry. If null this entry is an overlapping entry.
            /// </summary>
            public TimeSpan? Gap { get; }

            public long LogItems { get; }

            public bool? NodeRestart { get; }
    }

    #endregion


    #region Members

        struct LogDTInfo
        {
            public DateTimeOffsetRange LogDateRange;
            public UnitOfMeasure LogFileSize;
            public int LogItems;
            public DateTimeOffsetRange LogFileDateRange;
            public IFilePath LogFile;
            public bool Restart;
        }

        static IEnumerable<LogDTInfo> DetermineLogRange(IEnumerable<LogFileInfo> logItems)
        {
            var logRanges = new List<LogDTInfo>();

            foreach(var logItem in logItems)
            {
                if(logItem.Restarts.IsEmpty())
                {
                    logRanges.Add(new LogDTInfo()
                                    {
                                        LogDateRange = logItem.LogDateRange,
                                        LogFile = logItem.LogFile,
                                        LogFileDateRange = logItem.LogFileDateRange,
                                        LogFileSize = logItem.LogFileSize,
                                        LogItems = logItem.LogItems
                                    });
                    continue;
                }

                var dtStart = logItem.LogDateRange.Min;
                //var dtFileStart = logItem.LogFileDateRange.Min;
                DateTimeOffsetRange currentDT;

                for(int nIdx = 0; nIdx < logItem.Restarts.Count; nIdx++)
                {
                    currentDT = logItem.Restarts[nIdx];

                    logRanges.Add(new LogDTInfo()
                                    {
                                        LogDateRange = new DateTimeOffsetRange(dtStart, currentDT.Min),
                                        LogFile = logItem.LogFile,
                                        LogFileDateRange = logItem.LogFileDateRange,
                                        LogFileSize = UnitOfMeasure.ZeroValue,
                                        LogItems = 0,
                                        Restart = true
                                    });
                    dtStart = currentDT.Max;
                }

                logRanges.Add(new LogDTInfo()
                {
                    LogDateRange = new DateTimeOffsetRange(dtStart, logItem.LogDateRange.Max),
                    LogFile = logItem.LogFile,
                    LogFileDateRange = logItem.LogFileDateRange,
                    LogFileSize = logItem.LogFileSize,
                    LogItems = logItem.LogItems,
                    Restart = true
                });
            }

            return logRanges;
        }

        public override IEnumerable<IAggregatedStats> ComputeStats()
        {
            var logStats = new List<LogInfoStat>();

            try
            {
                TimeSpan? gaptimeSpan = null;
                DateTimeOffsetRange contTimeRange = null;
                TimeSpan contTimeSpan = new TimeSpan();
                int contTimeCnt = 0;
                UnitOfMeasure contSize = new UnitOfMeasure(UnitOfMeasure.Types.Storage | UnitOfMeasure.Types.MiB);
                long contLogItems = 0;
                LogInfoStat.OverLappingTypes overlappingType = LogInfoStat.OverLappingTypes.Normal;
                int nbrItems = 0;

                foreach (var node in this.Cluster.Nodes)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();
                    nbrItems = 0;

                    Logger.Instance.InfoFormat("Analyzing Log Information for Node \"{0}\"", node);
                    var nodeLogStats = new List<LogInfoStat>();
                    var logItems = from item in node.LogFiles
                                   let isDebugFile = item.IsDebugLog
                                   group item by isDebugFile into g
                                   select new { IsDebugFile = g.Key, LogInfos = DetermineLogRange(g).OrderBy(l => l.LogDateRange).ToList() };

                    foreach (var fileType in logItems)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        for (int nIdx = 0; nIdx < fileType.LogInfos.Count; ++nIdx)
                        {
                            this.CancellationToken.ThrowIfCancellationRequested();

                            var logInfo = fileType.LogInfos[nIdx];

                            if (nIdx > 0)
                            {
                                if (logInfo.LogDateRange == fileType.LogInfos[nIdx - 1].LogDateRange || fileType.LogInfos[nIdx - 1].LogDateRange.IsWithIn(logInfo.LogDateRange.Min))
                                {
                                    overlappingType = LogInfoStat.OverLappingTypes.Duplicated;
                                    gaptimeSpan = null;
                                }
                                else if (fileType.LogInfos[nIdx - 1].LogDateRange.Includes(logInfo.LogDateRange.Min))
                                {
                                    overlappingType = LogInfoStat.OverLappingTypes.OverLapping;
                                    gaptimeSpan = null;
                                }
                                else
                                {
                                    gaptimeSpan = (TimeSpan?)(logInfo.LogDateRange.Min - fileType.LogInfos[nIdx - 1].LogDateRange.Max);
                                    overlappingType = gaptimeSpan.Value.TotalMinutes <= LibrarySettings.LogFileInfoAnalysisGapTriggerInMins
                                                        ? LogInfoStat.OverLappingTypes.Normal
                                                        : LogInfoStat.OverLappingTypes.Gap;
                                }
                                ++contTimeCnt;
                            }
                            else
                            {
                                gaptimeSpan = TimeSpan.Zero;
                                contTimeRange = new DateTimeOffsetRange();
                                contTimeSpan = new TimeSpan();
                                overlappingType = LogInfoStat.OverLappingTypes.Normal;
                                contSize = contSize.Zero();
                                contLogItems = 0;
                                contTimeCnt = 0;
                            }

                            if (gaptimeSpan.HasValue)
                            {
                                if (Math.Abs(gaptimeSpan.Value.TotalMinutes) <= LibrarySettings.LogFileInfoAnalysisGapTriggerInMins)
                                {
                                    contTimeSpan = contTimeSpan.Add(logInfo.LogDateRange.TimeSpan());
                                    contTimeRange.SetMinimal(logInfo.LogDateRange.Min);
                                    contTimeRange.SetMaximum(logInfo.LogDateRange.Max);
                                    contSize = contSize.Add(logInfo.LogFileSize);
                                    contLogItems += logInfo.LogItems;                                   
                                }
                                else
                                {
                                    if (contTimeSpan.TotalDays >= LibrarySettings.LogFileInfoAnalysisContinousEventInDays)
                                    {
                                        nodeLogStats.Add(new LogInfoStat(node,
                                                                        null,
                                                                        contTimeRange,
                                                                        null,
                                                                        contSize,
                                                                        LogInfoStat.OverLappingTypes.ContinuousThresholdMet,
                                                                        fileType.IsDebugFile,
                                                                        contLogItems));
                                    }                                    

                                    contTimeRange = new DateTimeOffsetRange(logInfo.LogDateRange);
                                    contTimeSpan = logInfo.LogDateRange.TimeSpan();
                                    contSize = contSize.Zero().Add(logInfo.LogFileSize);
                                    contLogItems = logInfo.LogItems;
                                    contTimeCnt = 0;
                                }
                            }

                            nodeLogStats.Add(new LogInfoStat(node,
                                                            logInfo.LogFileDateRange,
                                                            logInfo.LogDateRange,
                                                            logInfo.LogFile,
                                                            logInfo.LogFileSize,
                                                            overlappingType,
                                                            fileType.IsDebugFile,
                                                            logInfo.LogItems,
                                                            gaptimeSpan,
                                                            logInfo.Restart ? (bool?) true : null));
                            ++nbrItems;
                        }

                        if (contTimeSpan.TotalDays >= LibrarySettings.LogFileInfoAnalysisContinousEventInDays)
                        {
                            nodeLogStats.Add(new LogInfoStat(node,
                                                            null,
                                                            contTimeRange,
                                                            null,
                                                            contSize,
                                                            contTimeCnt > 1
                                                                ? LogInfoStat.OverLappingTypes.ContinuousThresholdMet
                                                                : LogInfoStat.OverLappingTypes.DurationThresholdMet,
                                                            fileType.IsDebugFile,
                                                            contLogItems));
                        }                        
                    }

                    logStats.AddRange(nodeLogStats);

                    //Determine Log period durations
                    {
                        TimeSpan minTzOffset = TimeSpan.Zero;
                        TimeSpan maxTZOffset = TimeSpan.Zero;                                            
                        var systemLogFiles = nodeLogStats
                                                   .Where(l => l.LogItems > 0 && !l.IsDebugFile);
                        var debugLogFiles = nodeLogStats
                                                    .Where(l => l.LogItems > 0 && l.IsDebugFile);

                        var systemLogEntries = systemLogFiles;

                        if (systemLogEntries.HasAtLeastOneElement())
                        {
                            var systemMaxLogTS = systemLogEntries.Max(l => l.LogRange.Max);
                            var systemMinLogTS = systemLogEntries.Min(l => l.LogRange.Min);
                            var systemDuration = TimeSpan.FromSeconds(systemLogEntries
                                                                        .Where(l => l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Normal)
                                                                        .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                        .DefaultIfEmpty().Sum());
                            var systemGap = TimeSpan.FromSeconds(systemLogEntries
                                                                    .Where(l => l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Gap)
                                                                    .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                    .DefaultIfEmpty().Sum());

                            node.DSE.LogSystemDateRange = new DateTimeOffsetRange(systemMinLogTS, systemMaxLogTS);
                            node.DSE.LogSystemDuration = systemDuration;
                            node.DSE.LogSystemGap = systemGap;
                        }
                        node.DSE.LogSystemFiles = systemLogEntries.Count();
                        
                        var debugLogEntries = debugLogFiles;

                        if (debugLogEntries.HasAtLeastOneElement())
                        {
                            var debugMaxLogTS = debugLogEntries.Max(l => l.LogRange.Max);
                            var debugMinLogTS = debugLogEntries.Min(l => l.LogRange.Min);
                            var debugDuration = TimeSpan.FromSeconds(debugLogEntries
                                                                        .Where(l => l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Normal)
                                                                        .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                        .DefaultIfEmpty().Sum());
                            var debugGap = TimeSpan.FromSeconds(debugLogEntries
                                                                    .Where(l => l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Gap)
                                                                    .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                    .DefaultIfEmpty().Sum());

                            node.DSE.LogDebugDateRange = new DateTimeOffsetRange(debugMinLogTS, debugMaxLogTS);
                            node.DSE.LogDebugDuration = debugDuration;
                            node.DSE.LogDebugGap = debugGap;                            
                        }
                        node.DSE.LogDebugFiles = debugLogEntries.Count();                        
                    }

                    Logger.Instance.InfoFormat("Analyzed Log Information for Node \"{0}\", Total Nbr Items {1:###,###,##0}", node, nbrItems);
                }

                foreach (DataCenter dc in this.Cluster.DataCenters)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();
                    
                    Logger.Instance.InfoFormat("Analyzing Log Information for DC \"{0}\"", dc);

                    var nodeUpTime = dc.Nodes.Where(n => !n.DSE.Uptime.NaN)
                                                .Select(n => n.DSE.Uptime.ConvertTimeUOM(UnitOfMeasure.Types.MS))
                                                .ToArray();
                    var nodeUpTimeAvgMS = nodeUpTime.DefaultIfEmpty()
                                                    .Average();
                    var logSys = dc.Nodes.Where(n => n.DSE.LogSystemDuration.HasValue)
                                            .Select(n => n.DSE.LogSystemDuration.Value.TotalMilliseconds)
                                            .ToArray();
                    var logSysAvgMS = logSys.DefaultIfEmpty()
                                            .Average();
                    var logDebug = dc.Nodes.Where(n => n.DSE.LogDebugDuration.HasValue)
                                            .Select(n => n.DSE.LogDebugDuration.Value.TotalMilliseconds)
                                            .ToArray();
                    var logDebugAvgMS = logDebug.DefaultIfEmpty()
                                            .Average();
                    var logSysGapMS = dc.Nodes.Where(n => n.DSE.LogSystemGap.HasValue)
                                            .Select(n => n.DSE.LogSystemGap.Value.TotalMilliseconds)
                                            .DefaultIfEmpty()
                                            .Sum();
                    var logDebugGapMS = dc.Nodes.Where(n => n.DSE.LogDebugGap.HasValue)
                                            .Select(n => n.DSE.LogDebugGap.Value.TotalMilliseconds)
                                            .DefaultIfEmpty()
                                            .Sum();
                    var logSysFiles = dc.Nodes.Select(n => n.DSE.LogSystemFiles)
                                            .DefaultIfEmpty()
                                            .Sum();
                    var logDebugFiles = dc.Nodes.Select(n => n.DSE.LogDebugFiles)
                                            .DefaultIfEmpty()
                                            .Sum();

                    if (nodeUpTimeAvgMS > 0m)
                    {
                        dc.NodeUpTimeAvg = TimeSpan.FromMilliseconds((double)nodeUpTimeAvgMS);
                        dc.NodeUpTimeStdRange = TimeSpan.FromMilliseconds(nodeUpTime.StandardDeviationP() / Math.Sqrt(nodeUpTime.Length));
                    }
                    if (logSysAvgMS > 0d)
                    {
                        dc.LogSystemDurationAvg = TimeSpan.FromMilliseconds(logSysAvgMS);
                        dc.LogSystemDurationStdRange = TimeSpan.FromMilliseconds(logSys.StandardDeviationP() / Math.Sqrt(logSys.Length));
                    }
                    if (logDebugAvgMS > 0d)
                    {
                        dc.LogDebugDurationAvg = TimeSpan.FromMilliseconds(logDebugAvgMS);
                        dc.LogDebugDurationStdRange = TimeSpan.FromMilliseconds(logDebug.StandardDeviationP() / Math.Sqrt(logDebug.Length));
                    }
                    if (logSysGapMS > 0d)
                    {
                        dc.LogSystemGap = TimeSpan.FromMilliseconds(logSysGapMS);
                    }
                    if (logDebugGapMS > 0d)
                    {
                        dc.LogDebugGap = TimeSpan.FromMilliseconds(logDebugGapMS);
                    }

                    dc.LogSystemFiles = logSysFiles;
                    dc.LogDebugFiles = logDebugFiles;

                    Logger.Instance.InfoFormat("Analyzing Log Information for DC \"{0}\" Completed", dc);
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Analyzing Log Information Canceled");
            }

            return logStats;
        }

    #endregion

    }
}
