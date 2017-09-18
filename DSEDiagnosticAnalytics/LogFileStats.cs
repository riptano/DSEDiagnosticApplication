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

            public enum OverLappingTypes
            {
                Normal = 0,
                /// <summary>
                /// There is a gap in time between this entry and the prior
                /// </summary>
                Gap,
                /// <summary>
                /// This entry is a duplicate based on the prior entry
                /// </summary>
                Duplicated,
                /// <summary>
                /// This entry is a overlapping based on the prior entry
                /// </summary>
                OverLapping,
                /// <summary>
                /// This entry represents a series of continuous log files for a node (no gaps between log files)
                /// </summary>
                Continuous
            }

            public LogInfoStat(INode node,
                                DateTimeOffsetRange logFileRange,
                                DateTimeOffsetRange logRange,
                                IFilePath logFilePath,
                                UnitOfMeasure logFileSize,
                                OverLappingTypes overlappingType,
                                bool isDebugFile,
                                long logItems,
                                TimeSpan? gapLogDuration = null)
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

            public DSEInfo.InstanceTypes Product { get; }
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
    }

    #endregion


    #region Members

        public override IEnumerable<IAggregatedStats> ComputeStats()
        {
            var logStats = new List<LogInfoStat>();

            try
            {
                TimeSpan? gaptimeSpan = null;
                DateTimeOffsetRange contTimeRange = null;
                TimeSpan contTimeSpan = new TimeSpan();
                UnitOfMeasure contSize = new UnitOfMeasure(UnitOfMeasure.Types.Storage | UnitOfMeasure.Types.MiB);
                long contLogItems = 0;
                LogInfoStat.OverLappingTypes overlappingType = LogInfoStat.OverLappingTypes.Normal;
                int nbrItems = 0;

                foreach (var node in this.Cluster.Nodes)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();
                    nbrItems = 0;

                    Logger.Instance.InfoFormat("Analyzing Log Information for Node \"{0}\"", node);
                    var logItems = from item in node.LogFiles
                                   let isDebugFile = item.LogFile.Name.IndexOf("debug", StringComparison.OrdinalIgnoreCase) >= 0
                                   group item by isDebugFile into g
                                   select new { IsDebugFile = g.Key, LogInfos = g.OrderBy(l => l.LogDateRange).ToList() };

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
                            }
                            else
                            {
                                gaptimeSpan = TimeSpan.Zero;
                                contTimeRange = new DateTimeOffsetRange();
                                contTimeSpan = new TimeSpan();
                                overlappingType = LogInfoStat.OverLappingTypes.Normal;
                                contSize.Reset();
                                contLogItems = 0;
                            }

                            if (gaptimeSpan.HasValue)
                            {
                                if (Math.Abs(gaptimeSpan.Value.TotalMinutes) <= LibrarySettings.LogFileInfoAnalysisGapTriggerInMins)
                                {
                                    contTimeSpan = contTimeSpan.Add(logInfo.LogDateRange.TimeSpan());
                                    contTimeRange.SetMinimal(logInfo.LogDateRange.Min);
                                    contTimeRange.SetMaximum(logInfo.LogDateRange.Max);
                                    contSize.Add(logInfo.LogFileSize);
                                    contLogItems += logInfo.LogItems;
                                }
                                else
                                {
                                    if (contTimeSpan.TotalDays >= LibrarySettings.LogFileInfoAnalysisContinousEventInDays)
                                    {
                                        logStats.Add(new LogInfoStat(node,
                                                                        null,
                                                                        contTimeRange,
                                                                        null,
                                                                        contSize,
                                                                        LogInfoStat.OverLappingTypes.Continuous,
                                                                        fileType.IsDebugFile,
                                                                        contLogItems));
                                    }

                                    contTimeRange = new DateTimeOffsetRange(logInfo.LogDateRange);
                                    contTimeSpan = logInfo.LogDateRange.TimeSpan();
                                    contSize.Reset().Add(logInfo.LogFileSize);
                                    contLogItems = logInfo.LogItems;
                                }
                            }

                            logStats.Add(new LogInfoStat(node,
                                                            logInfo.LogFileDateRange,
                                                            logInfo.LogDateRange,
                                                            logInfo.LogFile,
                                                            logInfo.LogFileSize,
                                                            overlappingType,
                                                            fileType.IsDebugFile,
                                                            logInfo.LogItems,
                                                            gaptimeSpan));
                            ++nbrItems;
                        }

                        if (contTimeSpan.TotalDays >= LibrarySettings.LogFileInfoAnalysisContinousEventInDays)
                        {
                            logStats.Add(new LogInfoStat(node,
                                                            null,
                                                            contTimeRange,
                                                            null,
                                                            contSize,
                                                            LogInfoStat.OverLappingTypes.Continuous,
                                                            fileType.IsDebugFile,
                                                            contLogItems));
                        }
                    }

                    Logger.Instance.InfoFormat("Analyzed Log Information for Node \"{0}\", Total Nbr Items {1:###,###,##0}", node, nbrItems);
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
