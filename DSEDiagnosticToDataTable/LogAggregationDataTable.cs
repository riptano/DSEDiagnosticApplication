using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticToDataTable
{
    public sealed class LogAggregationDataTable : DataTableLoad
    {
        public LogAggregationDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, TimeSpan? aggregationPeriod = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            if (ignoreKeySpaces != null && ignoreKeySpaces.Length > 0) this.IgnoreKeySpaces = ignoreKeySpaces;
            if (aggregationPeriod.HasValue && aggregationPeriod > TimeSpan.Zero) this.AggregationPeriod = aggregationPeriod.Value;
        }

        public string[] IgnoreKeySpaces { get; }
        public TimeSpan AggregationPeriod { get; } = DSEDiagnosticAnalytics.LibrarySettings.LogAggregationPeriod;

        public struct Columns
        {
            public const string AggregationPeriod = "Aggregation Period";
            public const string Class = "Class";
            public const string RelatedInfo = "Related Info";
            public const string HasOrphanedEvents = "HasOrphanedEvents";
            public const string Path = "Path";
            public const string Exception = "Exception";
            public const string LastOccurrenceUTC = "Last Occurrence (UTC)";
            public const string LastOccurrenceLocal = "Last Occurrence (Local)";
            public const string Occurrences = "Occurrences";
            public const string UOM = "UOM";
            public const string DurationMax = "Duration Max";
            public const string DurationMin = "Duration Min";
            public const string DurationMean = "Duration Mean";
            public const string DurationStdDevp = "Duration StdDevp";
            public const string DurationTotal = "Duration Total";
            public const string RateMax = "Rate Max";
            public const string RateMin = "Rate Min";
            public const string RateMean = "Rate Mean";
            public const string RateStdDevp = "Rate StdDevp";
            public const string PendingTombstone = "Pending|Tombstone";
            public const string CompletedLive = "Completed|Live";
            public const string Blocked = "Blocked";
            public const string AllTimeBlocked = "AllTimeBlocked";
            public const string EdenThresholdReadRateMax = "EdenThresholdReadRateMax";
            public const string EdenThresholdReadRateMin = "EdenDifThresholdReadRateMin";
            public const string OldThresholdReadRateMean = "OldThresholdReadRateMean";
            public const string OldThresholdReadRateStdDevp = "OldThresholdReadRateStdDevp";
            public const string SurvivorMax = "Survivor Max";
            public const string SurvivorDif = "Survivor Dif";
            public const string SizeMax = "Size Max";
            public const string SizeMin = "Size Min";
            public const string SizeMean = "Size Mean";
            public const string SizeStdDevp = "Size StdDevp";
            public const string SizeTotal = "Size Total";
            public const string CountMax = "Count Max";
            public const string CountMin = "Count Min";
            public const string CountMean = "Count Mean";
            public const string CountStdDevp = "Count StdDevp";
            public const string CountTotal = "Count Total";
            public const string OPSMax = "OPS Max";
            public const string OPSMin = "OPS Min";
            public const string OPSMean = "OPS Mean";
            public const string OPSStdDevp = "OPS StdDevp";
            public const string SerializedMax = "Serialized Max";
            public const string SerializedMin = "Serialized Min";
            public const string SerializedMean = "Serialized Mean";
            public const string SerializedStdDevp = "Serialized StdDevp";
            public const string SerializedTotal = "Serialized Total";
            public const string StorageMax = "Storage Max";
            public const string StorageMin = "Storage Min";
            public const string StorageMean = "Storage Mean";
            public const string StorageStdDevp = "Storage StdDevp";
            public const string StorageTotal = "Storage Total";
        }

        public override DataTable CreateInitializationTable()
        {
            var dtLog = new DataTable(TableNames.LogAggregation, TableNames.Namespace);

            if (this.SessionId.HasValue) dtLog.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtLog.Columns.Add(ColumnNames.UTCTimeStamp, typeof(DateTime)); 
            dtLog.Columns.Add(ColumnNames.LogLocalTimeStamp, typeof(DateTime)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.AggregationPeriod, typeof(TimeSpan));
            dtLog.Columns.Add(ColumnNames.DataCenter, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.KeySpace, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.Class, typeof(string));
            dtLog.Columns.Add(Columns.RelatedInfo, typeof(string));
            dtLog.Columns.Add(Columns.HasOrphanedEvents, typeof(bool)).AllowDBNull = true; 
            dtLog.Columns.Add(Columns.Path, typeof(string)); //Exception Path
            dtLog.Columns.Add(Columns.Exception, typeof(string)).AllowDBNull = true; 

            dtLog.Columns.Add(Columns.LastOccurrenceUTC, typeof(DateTime)); 
            dtLog.Columns.Add(Columns.LastOccurrenceLocal, typeof(DateTime));

            dtLog.Columns.Add(Columns.Occurrences, typeof(long));

            dtLog.Columns.Add(Columns.UOM, typeof(string));

            dtLog.Columns.Add(Columns.DurationMax, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.DurationMin, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.DurationMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.DurationStdDevp, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.DurationTotal, typeof(decimal)).AllowDBNull = true;
        
            dtLog.Columns.Add(Columns.RateMax, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.RateMin, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.RateMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.RateStdDevp, typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add(Columns.PendingTombstone, typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.CompletedLive, typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.Blocked, typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.AllTimeBlocked, typeof(long)).AllowDBNull = true;

            dtLog.Columns.Add(Columns.EdenThresholdReadRateMax, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.EdenThresholdReadRateMin, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.OldThresholdReadRateMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.OldThresholdReadRateStdDevp, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SurvivorMax, typeof(decimal)).AllowDBNull = true;                        
            dtLog.Columns.Add(Columns.SurvivorDif, typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add(Columns.SizeMax, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SizeMin, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SizeMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SizeStdDevp, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SizeTotal, typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add(Columns.CountMax, typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.CountMin, typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.CountMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.CountStdDevp, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.CountTotal, typeof(long)).AllowDBNull = true;

            dtLog.Columns.Add(Columns.OPSMax, typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.OPSMin, typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.OPSMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.OPSStdDevp, typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add(Columns.SerializedMax, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SerializedMin, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SerializedMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SerializedStdDevp, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.SerializedTotal, typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add(Columns.StorageMax, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.StorageMin, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.StorageMean, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.StorageStdDevp, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(Columns.StorageTotal, typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.ReconciliationRef, typeof(string)).AllowDBNull = true;

            //dtLog.PrimaryKey = new System.Data.DataColumn[] { dtLog.Columns[ColumnNames.KeySpace], dtLog.Columns["Name"] };

            dtLog.DefaultView.ApplyDefaultSort = false;
            dtLog.DefaultView.AllowDelete = false;
            dtLog.DefaultView.AllowEdit = false;
            dtLog.DefaultView.AllowNew = false;
            /*dtLog.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [{2}] ASC, [{3}] ASC, [{4}] ASC",
                                                    ColumnNames.UTCTimeStamp,
                                                    ColumnNames.DataCenter,
                                                    ColumnNames.NodeIPAddress,
                                                    ColumnNames.KeySpace,
                                                    ColumnNames.Table);
                                                    */
            return dtLog;
        }

        struct LogEvtGroup
        {
            public DSEDiagnosticAnalytics.LogEventGroup GroupKey;
            public bool HasOrphaned;
            public ILogEvent LastEvent;
            public ILogEvent FirstEvent;
            public int NbrOccurs;
            public string SessionTieOut;
            public IEnumerable<DSEDiagnosticAnalytics.LogEventGrouping> AnalyticsGroupings;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        public override DataTable LoadTable()
        {
            Logger.Instance.InfoFormat("Log Aggregation Processing Started. Aggregation Period of {0}", this.AggregationPeriod);

            this.Table.BeginLoadData();
            try
            {
                var aggPeriods = new List<DateTimeRange>();

                #region Determine log Aggregation Period 
                {
                    var logDateRange = new DateTimeOffsetRange();

                    this.Cluster.Nodes.SelectMany(n => n.LogFiles).ForEach(logFile => logDateRange.SetMinMax(logFile.LogDateRange));

                    if (logDateRange.IsEmpty()) return this.Table;

                    var beginAggDateTime = logDateRange.Min.UtcDateTime.RoundUp(this.AggregationPeriod) - this.AggregationPeriod;
                    var endingAggDateTime = logDateRange.Max.UtcDateTime.RoundUp(this.AggregationPeriod);

                    for (DateTime beginDateTime = beginAggDateTime, endDateTime = beginAggDateTime + this.AggregationPeriod;
                            beginDateTime < endingAggDateTime;)
                    {
                        aggPeriods.Add(new DateTimeRange(beginDateTime, endDateTime));
                        beginDateTime = endDateTime;
                        endDateTime += this.AggregationPeriod;
                    }
                }
                #endregion

                #region Aggregation Log Events

                //Merge logs from all nodes and determine events used for aggregations
                /*var logGrpEvts = from logEvent in this.Cluster.Nodes.AsParallel().SelectMany(n => 
                                                    (from logMMV in n.LogEventsCache(LogCassandraEvent.ElementCreationTypes.EventTypeOnly, true)
                                                     let logEvt = logMMV.Value
                                                     where (logEvt.Type & EventTypes.SingleInstance) != 0
                                                             || (logEvt.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                                                             || (logEvt.Type & EventTypes.SessionDefinedByDuration) == EventTypes.SessionDefinedByDuration
                                                             || (logEvt.Type & EventTypes.AggregateData) == EventTypes.AggregateData
                                                             || ((logEvt.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin && (logEvt.Class & EventClasses.Orphaned) == EventClasses.Orphaned)
                                                     let aggregationDateTime = aggPeriods.First(p => p.Includes(logEvt.EventTime.UtcDateTime))
                                                     let logEvent = logMMV.GetValue((Common.Patterns.Collections.MemoryMapperElementCreationTypes)LogCassandraEvent.ElementCreationTypes.AggregationPeriodOnlyWProps)
                                                     select new
                                                     {
                                                         AggregationDateTime = aggregationDateTime,
                                                         GroupKey = new DSEDiagnosticAnalytics.LogEventGroup(aggregationDateTime, logEvent),
                                                         LogEvent = logEvent
                                                     }))                                                                  
                                group logEvent by logEvent.GroupKey
                                        into g
                                let grpEvents = g.Select(x => x.LogEvent).OrderBy(e => e.EventTime)
                                 select new { GroupKey = g.Key,
                                                HasOrphaned = grpEvents.Any(e => (e.Class & EventClasses.Orphaned) == EventClasses.Orphaned),
                                                LastEvent = grpEvents.Last(),
                                                FirstEvent = grpEvents.First(),
                                                NbrOccurs = g.Count(),
                                                AnalyticsGroupings = DSEDiagnosticAnalytics.LogEventGrouping.CreateLogEventGrouping(g.Key, grpEvents)
                                 };

 */
                this.CancellationToken.ThrowIfCancellationRequested();

                var nodeGrpEvents = new CTS.List<IEnumerable<LogEvtGroup>>(this.Cluster.Nodes.Count());
                var parallelOptions = new System.Threading.Tasks.ParallelOptions()
                {
                    CancellationToken = this.CancellationToken,
                    TaskScheduler = new Schedulers.WorkStealingTaskScheduler(this.Cluster.Nodes.Count())
                };

                
                System.Threading.Tasks.Parallel.ForEach(this.Cluster.Nodes, parallelOptions, node =>
                {
                    Logger.Instance.InfoFormat("Log Aggregation Processing for {0} Started", node.Id.NodeName());
                    
                    var nodeEvtGrps = (from logMMV in node.LogEventsCache(LogCassandraEvent.ElementCreationTypes.EventTypeOnly, true, false)
                                        let logEvt = logMMV.Value
                                        where (logEvt.Type & EventTypes.ExceptionElement) == EventTypes.ExceptionElement
                                                || (logEvt.Type & EventTypes.SingleInstance) == EventTypes.SingleInstance
                                                || (logEvt.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                                                || (logEvt.Type & EventTypes.SessionDefinedByDuration) == EventTypes.SessionDefinedByDuration
                                                || (logEvt.Type & EventTypes.AggregateData) == EventTypes.AggregateData
                                                || ((logEvt.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin && (logEvt.Class & EventClasses.Orphaned) == EventClasses.Orphaned)
                                        let aggregationDateTime = aggPeriods.First(p => p.Includes(logEvt.EventTime.UtcDateTime))
                                        let logEvent = logMMV.GetValue((Common.Patterns.Collections.MemoryMapperElementCreationTypes)LogCassandraEvent.ElementCreationTypes.AggregationPeriodOnlyWProps)
                                        let groupKey = new DSEDiagnosticAnalytics.LogEventGroup(aggregationDateTime, logEvent)
                                        group logEvent by groupKey
                                            into g
                                        let grpEvents = g.OrderBy(e => e.EventTime).ToArray()
                                        let SessionTieOuts = grpEvents
                                                                .Where(t => (t.Class & EventClasses.Repair) == EventClasses.Repair)
                                                                .Select(t => t.Id)
                                                                .DuplicatesRemoved(i => i)
                                       select new LogEvtGroup()
                                        {
                                            GroupKey = g.Key,
                                            HasOrphaned = grpEvents.Any(e => (e.Class & EventClasses.Orphaned) == EventClasses.Orphaned),
                                            LastEvent = grpEvents.Last(),
                                            FirstEvent = grpEvents.First(),
                                            SessionTieOut = SessionTieOuts.HasAtLeastOneElement()
                                                                ? string.Join(",",SessionTieOuts)
                                                                : null,
                                            NbrOccurs = g.Count(),
                                            AnalyticsGroupings = DSEDiagnosticAnalytics.LogEventGrouping.CreateLogEventGrouping(g.Key, grpEvents)
                                        }).ToArray();

                    Logger.Instance.InfoFormat("Log Aggregation Processing for {0} completed with {1:###,###,##0}", node.Id.NodeName(), nodeEvtGrps.Length);

                    nodeGrpEvents.Add(nodeEvtGrps);
                }
                );

                this.CancellationToken.ThrowIfCancellationRequested();

                DataRow dataRow = null;
                int nbrItems = 0;

                Logger.Instance.InfoFormat("Log Aggregation Processing Started for Sort/Datatable for {0} nodes", nodeGrpEvents.UnSafe.Count);
                
                foreach (var logGrpEvt in nodeGrpEvents.UnSafe.SelectMany(e => e)
                                            .OrderBy(i => i.GroupKey.AggregationDateTime)
                                                .ThenBy(i => i.FirstEvent.EventTime)
                                                .ThenBy(i => i.LastEvent.EventTime))
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    if(logGrpEvt.AnalyticsGroupings.IsEmpty()) continue;

                    string unitOfMeasure = string.Empty;

                    dataRow = this.Table.NewRow();

                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetField(ColumnNames.UTCTimeStamp, logGrpEvt.GroupKey.AggregationDateTime);
                    dataRow.SetField(ColumnNames.LogLocalTimeStamp, logGrpEvt.GroupKey.AggregationDateTime + logGrpEvt.LastEvent.EventTime.Offset);
                    dataRow.SetField(Columns.AggregationPeriod, this.AggregationPeriod);
                    dataRow.SetField(ColumnNames.DataCenter, logGrpEvt.GroupKey.DataCenterName);
                    dataRow.SetField(ColumnNames.NodeIPAddress, logGrpEvt.GroupKey.NodeName);
                    dataRow.SetField(ColumnNames.KeySpace, logGrpEvt.GroupKey.KeySpaceName);
                    dataRow.SetField(ColumnNames.Table, logGrpEvt.GroupKey.TableName);
                    dataRow.SetField(Columns.Class, logGrpEvt.GroupKey.Class);
                    dataRow.SetField(Columns.RelatedInfo, logGrpEvt.GroupKey.AssocItem);
                    if(logGrpEvt.HasOrphaned) dataRow.SetField(Columns.HasOrphanedEvents, true);
                    dataRow.SetField(Columns.Path, logGrpEvt.GroupKey.Path);
                    dataRow.SetField(Columns.Exception, logGrpEvt.GroupKey.Exception);
                    dataRow.SetField(Columns.LastOccurrenceUTC, logGrpEvt.LastEvent.EventTime.UtcDateTime);
                    dataRow.SetField(Columns.LastOccurrenceLocal, logGrpEvt.LastEvent.EventTimeLocal);
                    dataRow.SetField(Columns.Occurrences, logGrpEvt.NbrOccurs);
                    dataRow.SetField(ColumnNames.ReconciliationRef, logGrpEvt.SessionTieOut);

                    foreach (var analyticsGrp in logGrpEvt.AnalyticsGroupings)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if (analyticsGrp.DurationStats != null && analyticsGrp.DurationStats.HasValue)
                        {
                            dataRow.SetField(Columns.DurationMax, analyticsGrp.DurationStats.Duration.Max);
                            dataRow.SetField(Columns.DurationMin, analyticsGrp.DurationStats.Duration.Min);
                            dataRow.SetField(Columns.DurationMean, analyticsGrp.DurationStats.Duration.Mean);
                            dataRow.SetField(Columns.DurationStdDevp, analyticsGrp.DurationStats.Duration.StdDev);
                            dataRow.SetField(Columns.DurationTotal, analyticsGrp.DurationStats.Duration.Sum);
                            unitOfMeasure += analyticsGrp.DurationStats.UOM + ',';
                        }
                        if (analyticsGrp.PoolStats != null && analyticsGrp.PoolStats.HasValue)
                        {
                            if(analyticsGrp.PoolStats.Pending.HasValue)
                                dataRow.SetField(Columns.PendingTombstone, analyticsGrp.PoolStats.Pending.Sum);
                            if (analyticsGrp.PoolStats.Completed.HasValue)
                                dataRow.SetField(Columns.CompletedLive, analyticsGrp.PoolStats.Completed.Sum);
                            if (analyticsGrp.PoolStats.Blocked.HasValue)
                                dataRow.SetField(Columns.Blocked, analyticsGrp.PoolStats.Blocked.Sum);
                            if (analyticsGrp.PoolStats.AllTimeBlocked.HasValue)
                                dataRow.SetField(Columns.AllTimeBlocked, analyticsGrp.PoolStats.AllTimeBlocked.Sum);
                            unitOfMeasure += analyticsGrp.PoolStats.UOM + ',';
                        }
                        if (analyticsGrp.GCStats != null && analyticsGrp.GCStats.HasValue)
                        {
                            if (analyticsGrp.GCStats.Eden?.Difference.HasValue ?? false)
                            {
                                dataRow.SetField(Columns.EdenThresholdReadRateMax, Math.Max(analyticsGrp.GCStats.Eden.Before.Max, analyticsGrp.GCStats.Eden.After.Max));
                                dataRow.SetField(Columns.EdenThresholdReadRateMin, analyticsGrp.GCStats.Eden.Difference.Sum);
                            }
                            if (analyticsGrp.GCStats.Old?.Difference.HasValue ?? false)
                            {
                                dataRow.SetField(Columns.OldThresholdReadRateMean, Math.Max(analyticsGrp.GCStats.Old.Before.Max, analyticsGrp.GCStats.Old.After.Max));
                                dataRow.SetField(Columns.OldThresholdReadRateStdDevp, analyticsGrp.GCStats.Old.Difference.Sum);
                            }
                            if (analyticsGrp.GCStats.Survivor?.Difference.HasValue ?? false)
                            {
                                dataRow.SetField(Columns.SurvivorMax, Math.Max(analyticsGrp.GCStats.Survivor.Before.Max, analyticsGrp.GCStats.Survivor.After.Max));
                                dataRow.SetField(Columns.SurvivorDif, analyticsGrp.GCStats.Survivor.Difference.Sum);
                            }

                            if (analyticsGrp.GCStats.GCOccurrences.HasValue)
                            {
                                dataRow.SetField(Columns.CountMax, analyticsGrp.GCStats.GCOccurrences.Max);
                                dataRow.SetField(Columns.CountMin, analyticsGrp.GCStats.GCOccurrences.Min);
                                dataRow.SetField(Columns.CountMean, analyticsGrp.GCStats.GCOccurrences.Mean);
                                dataRow.SetField(Columns.CountStdDevp, analyticsGrp.GCStats.GCOccurrences.StdDev);
                                dataRow.SetField(Columns.CountTotal, analyticsGrp.GCStats.GCOccurrences.Sum);
                            }
                            if (analyticsGrp.GCStats.GCOccurrenceDuration.HasValue)
                            {
                                dataRow.SetField(Columns.OPSMax, analyticsGrp.GCStats.GCOccurrenceDuration.Max);
                                dataRow.SetField(Columns.OPSMin, analyticsGrp.GCStats.GCOccurrenceDuration.Min);
                                dataRow.SetField(Columns.OPSMean, analyticsGrp.GCStats.GCOccurrenceDuration.Mean);
                                dataRow.SetField(Columns.OPSStdDevp, analyticsGrp.GCStats.GCOccurrenceDuration.StdDev);
                            }

                            unitOfMeasure += analyticsGrp.GCStats.UOM + ',';
                        }
                        if(analyticsGrp.PoolMemTblOPSStats != null && analyticsGrp.PoolMemTblOPSStats.HasValue)
                        {
                            if(analyticsGrp.PoolMemTblOPSStats.OPS.HasValue)
                            {
                                dataRow.SetField(Columns.RateMax, analyticsGrp.PoolMemTblOPSStats.OPS.Max);
                                dataRow.SetField(Columns.RateMin, analyticsGrp.PoolMemTblOPSStats.OPS.Min);
                                dataRow.SetField(Columns.RateMean, analyticsGrp.PoolMemTblOPSStats.OPS.Mean);
                                dataRow.SetField(Columns.RateStdDevp, analyticsGrp.PoolMemTblOPSStats.OPS.StdDev);
                                //dataRow.SetField("Rate Total", analyticsGrp.PoolMemTblOPSStats.OPS.Sum);
                            }
                            if(analyticsGrp.PoolMemTblOPSStats.Size.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.PoolMemTblOPSStats.Size.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.PoolMemTblOPSStats.Size.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.PoolMemTblOPSStats.Size.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.PoolMemTblOPSStats.Size.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.PoolMemTblOPSStats.Size.Sum);
                            }
                            unitOfMeasure += analyticsGrp.PoolMemTblOPSStats.UOM + ',';
                        }
                        if (analyticsGrp.LargePartitionRowStats != null && analyticsGrp.LargePartitionRowStats.HasValue)
                        {                            
                            if (analyticsGrp.LargePartitionRowStats.Size.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.LargePartitionRowStats.Size.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.LargePartitionRowStats.Size.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.LargePartitionRowStats.Size.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.LargePartitionRowStats.Size.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.LargePartitionRowStats.Size.Sum);
                            }
                            unitOfMeasure += analyticsGrp.LargePartitionRowStats.UOM + ',';
                        }
                        if (analyticsGrp.TombstoneStats != null && analyticsGrp.TombstoneStats.HasValue)
                        {
                            if (analyticsGrp.TombstoneStats.Tombstones.HasValue)
                            {
                                dataRow.SetField(Columns.PendingTombstone, analyticsGrp.TombstoneStats.Tombstones.Sum);
                            }
                            if (analyticsGrp.TombstoneStats.LiveCells.HasValue)
                            {
                                dataRow.SetField(Columns.CompletedLive, analyticsGrp.TombstoneStats.LiveCells.Sum);
                            }
                            unitOfMeasure += analyticsGrp.TombstoneStats.UOM + ',';
                        }
                        if (analyticsGrp.GossipPendingStats != null && analyticsGrp.GossipPendingStats.HasValue)
                        {
                            if (analyticsGrp.GossipPendingStats.Pending.HasValue)
                            {
                                dataRow.SetField(Columns.PendingTombstone, analyticsGrp.GossipPendingStats.Pending.Sum);
                            }                            
                            unitOfMeasure += analyticsGrp.GossipPendingStats.UOM + ',';
                        }
                        if (analyticsGrp.BatchSizeStats != null && analyticsGrp.BatchSizeStats.HasValue)
                        {
                            if (analyticsGrp.BatchSizeStats.NbrPartitions.HasValue)
                            {
                                dataRow.SetField(Columns.CountMax, analyticsGrp.BatchSizeStats.NbrPartitions.Max);
                                dataRow.SetField(Columns.CountMin, analyticsGrp.BatchSizeStats.NbrPartitions.Min);
                                dataRow.SetField(Columns.CountMean, analyticsGrp.BatchSizeStats.NbrPartitions.Mean);
                                dataRow.SetField(Columns.CountStdDevp, analyticsGrp.BatchSizeStats.NbrPartitions.StdDev);
                                dataRow.SetField(Columns.CountTotal, analyticsGrp.BatchSizeStats.NbrPartitions.Sum);
                            }
                            if (analyticsGrp.BatchSizeStats.BatchSizes.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.BatchSizeStats.BatchSizes.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.BatchSizeStats.BatchSizes.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.BatchSizeStats.BatchSizes.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.BatchSizeStats.BatchSizes.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.BatchSizeStats.BatchSizes.Sum);
                            }
                            unitOfMeasure += analyticsGrp.BatchSizeStats.UOM + ',';
                        }
                        if (analyticsGrp.PreparesDiscardedStats != null && analyticsGrp.PreparesDiscardedStats.HasValue)
                        {
                            if (analyticsGrp.PreparesDiscardedStats.NbrPrepares.HasValue)
                            {
                                dataRow.SetField(Columns.CountMax, analyticsGrp.PreparesDiscardedStats.NbrPrepares.Max);
                                dataRow.SetField(Columns.CountMin, analyticsGrp.PreparesDiscardedStats.NbrPrepares.Min);
                                dataRow.SetField(Columns.CountMean, analyticsGrp.PreparesDiscardedStats.NbrPrepares.Mean);
                                dataRow.SetField(Columns.CountStdDevp, analyticsGrp.PreparesDiscardedStats.NbrPrepares.StdDev);
                                dataRow.SetField(Columns.CountTotal, analyticsGrp.PreparesDiscardedStats.NbrPrepares.Sum);
                            }
                            if (analyticsGrp.PreparesDiscardedStats.CacheSizes.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.PreparesDiscardedStats.CacheSizes.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.PreparesDiscardedStats.CacheSizes.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.PreparesDiscardedStats.CacheSizes.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.PreparesDiscardedStats.CacheSizes.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.PreparesDiscardedStats.CacheSizes.Sum);
                            }
                            unitOfMeasure += analyticsGrp.PreparesDiscardedStats.UOM + ',';
                        }
                        if (analyticsGrp.FlushStats != null && analyticsGrp.FlushStats.HasValue)
                        {
                            if (analyticsGrp.FlushStats.OPS.HasValue)
                            {
                                dataRow.SetField(Columns.OPSMax, analyticsGrp.FlushStats.OPS.Max);
                                dataRow.SetField(Columns.OPSMin, analyticsGrp.FlushStats.OPS.Min);
                                dataRow.SetField(Columns.OPSMean, analyticsGrp.FlushStats.OPS.Mean);
                                dataRow.SetField(Columns.OPSStdDevp, analyticsGrp.FlushStats.OPS.StdDev);                                
                            }

                            if (analyticsGrp.FlushStats.FlushSize.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.FlushStats.FlushSize.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.FlushStats.FlushSize.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.FlushStats.FlushSize.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.FlushStats.FlushSize.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.FlushStats.FlushSize.Sum);
                            }

                            if (analyticsGrp.FlushStats.FlushThreshold.HasValue)
                            {
                                dataRow.SetField(Columns.EdenThresholdReadRateMax, analyticsGrp.FlushStats.FlushThreshold.Max);
                                dataRow.SetField(Columns.EdenThresholdReadRateMin, analyticsGrp.FlushStats.FlushThreshold.Min);
                                dataRow.SetField(Columns.OldThresholdReadRateMean, analyticsGrp.FlushStats.FlushThreshold.Mean);
                                dataRow.SetField(Columns.OldThresholdReadRateStdDevp, analyticsGrp.FlushStats.FlushThreshold.StdDev);                                
                            }

                            if (analyticsGrp.FlushStats.SerializedSize.HasValue)
                            {
                                dataRow.SetField(Columns.SerializedMax, analyticsGrp.FlushStats.SerializedSize.Max);
                                dataRow.SetField(Columns.SerializedMin, analyticsGrp.FlushStats.SerializedSize.Min);
                                dataRow.SetField(Columns.SerializedMean, analyticsGrp.FlushStats.SerializedSize.Mean);
                                dataRow.SetField(Columns.SerializedStdDevp, analyticsGrp.FlushStats.SerializedSize.StdDev);
                                dataRow.SetField(Columns.SerializedTotal, analyticsGrp.FlushStats.SerializedSize.Sum);
                            }

                            if (analyticsGrp.FlushStats.StorageSize.HasValue)
                            {
                                dataRow.SetField(Columns.StorageMax, analyticsGrp.FlushStats.StorageSize.Max);
                                dataRow.SetField(Columns.StorageMin, analyticsGrp.FlushStats.StorageSize.Min);
                                dataRow.SetField(Columns.StorageMean, analyticsGrp.FlushStats.StorageSize.Mean);
                                dataRow.SetField(Columns.StorageStdDevp, analyticsGrp.FlushStats.StorageSize.StdDev);
                                dataRow.SetField(Columns.StorageTotal, analyticsGrp.FlushStats.StorageSize.Sum);
                            }

                            unitOfMeasure += analyticsGrp.FlushStats.UOM + ',';
                        }

                        if (analyticsGrp.CompactionStats != null && analyticsGrp.CompactionStats.HasValue)
                        {
                            if (analyticsGrp.CompactionStats.RowOPS.HasValue)
                            {
                                dataRow.SetField(Columns.OPSMax, analyticsGrp.CompactionStats.RowOPS.Max);
                                dataRow.SetField(Columns.OPSMin, analyticsGrp.CompactionStats.RowOPS.Min);
                                dataRow.SetField(Columns.OPSMean, analyticsGrp.CompactionStats.RowOPS.Mean);
                                dataRow.SetField(Columns.OPSStdDevp, analyticsGrp.CompactionStats.RowOPS.StdDev);
                            }

                            if (analyticsGrp.CompactionStats.NewSize.HasValue)
                            {
                                dataRow.SetField(Columns.StorageMax, analyticsGrp.CompactionStats.NewSize.Max);
                                dataRow.SetField(Columns.StorageMin, analyticsGrp.CompactionStats.NewSize.Min);
                                dataRow.SetField(Columns.StorageMean, analyticsGrp.CompactionStats.NewSize.Mean);
                                dataRow.SetField(Columns.StorageStdDevp, analyticsGrp.CompactionStats.NewSize.StdDev);
                                dataRow.SetField(Columns.StorageTotal, analyticsGrp.CompactionStats.NewSize.Sum);
                            }

                            if (analyticsGrp.CompactionStats.OldSize.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.CompactionStats.OldSize.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.CompactionStats.OldSize.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.CompactionStats.OldSize.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.CompactionStats.OldSize.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.CompactionStats.OldSize.Sum);
                            }

                            if (analyticsGrp.CompactionStats.ReadRate.HasValue)
                            {
                                dataRow.SetField(Columns.EdenThresholdReadRateMax, analyticsGrp.CompactionStats.ReadRate.Max);
                                dataRow.SetField(Columns.EdenThresholdReadRateMin, analyticsGrp.CompactionStats.ReadRate.Min);
                                dataRow.SetField(Columns.OldThresholdReadRateMean, analyticsGrp.CompactionStats.ReadRate.Mean);
                                dataRow.SetField(Columns.OldThresholdReadRateStdDevp, analyticsGrp.CompactionStats.ReadRate.StdDev);                               
                            }

                            if (analyticsGrp.CompactionStats.WriteRate.HasValue)
                            {
                                dataRow.SetField(Columns.RateMax, analyticsGrp.CompactionStats.WriteRate.Max);
                                dataRow.SetField(Columns.RateMin, analyticsGrp.CompactionStats.WriteRate.Min);
                                dataRow.SetField(Columns.RateMean, analyticsGrp.CompactionStats.WriteRate.Mean);
                                dataRow.SetField(Columns.RateStdDevp, analyticsGrp.CompactionStats.WriteRate.StdDev);
                            }

                            if (analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.HasValue)
                            {
                                dataRow.SetField(Columns.CountMax, analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Max);
                                dataRow.SetField(Columns.CountMin, analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Min);
                                dataRow.SetField(Columns.CountMean, analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Mean);
                                dataRow.SetField(Columns.CountStdDevp, analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.StdDev);
                                dataRow.SetField(Columns.CountTotal, analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Sum);
                            }

                            unitOfMeasure += analyticsGrp.CompactionStats.UOM + ',';
                        }

                        if (analyticsGrp.HintHandOffStats != null && analyticsGrp.HintHandOffStats.HasValue)
                        {
                            
                            if (analyticsGrp.HintHandOffStats.HintRows.HasValue)
                            {
                                dataRow.SetField(Columns.CountMax, analyticsGrp.HintHandOffStats.HintRows.Max);
                                dataRow.SetField(Columns.CountMin, analyticsGrp.HintHandOffStats.HintRows.Min);
                                dataRow.SetField(Columns.CountMean, analyticsGrp.HintHandOffStats.HintRows.Mean);
                                dataRow.SetField(Columns.CountStdDevp, analyticsGrp.HintHandOffStats.HintRows.StdDev);
                                dataRow.SetField(Columns.CountTotal, analyticsGrp.HintHandOffStats.HintRows.Sum);
                            }

                            unitOfMeasure += analyticsGrp.HintHandOffStats.UOM + ',';
                        }

                        if (analyticsGrp.MaximumMemoryUsageReachedStats != null && analyticsGrp.MaximumMemoryUsageReachedStats.HasValue)
                        {

                            if (analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Sum);
                            }

                            //if (analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.HasValue)
                            //{
                            //    dataRow.SetField(Columns.SizeMax, analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Max);
                            //    dataRow.SetField(Columns.SizeMin, analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Min);
                            //    dataRow.SetField(Columns.SizeMean, analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Mean);
                            //    dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.StdDev);
                            //    dataRow.SetField(Columns.SizeTotal, analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Sum);
                            //}

                            unitOfMeasure += analyticsGrp.MaximumMemoryUsageReachedStats.UOM + ',';
                        }

                        if (analyticsGrp.FreeDeviceStorageStats != null && analyticsGrp.FreeDeviceStorageStats.HasValue)
                        {
                            if (analyticsGrp.FreeDeviceStorageStats.FreeStorage.HasValue)
                            {
                                dataRow.SetField(Columns.StorageMax, analyticsGrp.FreeDeviceStorageStats.FreeStorage.Max);
                                dataRow.SetField(Columns.StorageMin, analyticsGrp.FreeDeviceStorageStats.FreeStorage.Min);
                                dataRow.SetField(Columns.StorageMean, analyticsGrp.FreeDeviceStorageStats.FreeStorage.Mean);
                                dataRow.SetField(Columns.StorageStdDevp, analyticsGrp.FreeDeviceStorageStats.FreeStorage.StdDev);
                                dataRow.SetField(Columns.StorageTotal, analyticsGrp.FreeDeviceStorageStats.FreeStorage.Sum);
                            }
                            
                            unitOfMeasure += analyticsGrp.FreeDeviceStorageStats.UOM + ',';
                        }

                        if (analyticsGrp.CompactionInsufficientSpaceStats != null && analyticsGrp.CompactionInsufficientSpaceStats.HasValue)
                        {
                            if (analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.HasValue)
                            {
                                dataRow.SetField(Columns.StorageMax, analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Max);
                                dataRow.SetField(Columns.StorageMin, analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Min);
                                dataRow.SetField(Columns.StorageMean, analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Mean);
                                dataRow.SetField(Columns.StorageStdDevp, analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.StdDev);
                                //dataRow.SetField(Columns.StorageTotal, analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Sum);
                            }

                            unitOfMeasure += analyticsGrp.CompactionInsufficientSpaceStats.UOM + ',';
                        }

                        if (analyticsGrp.DropStats != null && analyticsGrp.DropStats.HasValue)
                        {

                            if (analyticsGrp.DropStats.InternalDrops.HasValue) // || analyticsGrp.DropStats.CrossNodeDrops.HasValue)
                            {
                                dataRow.SetField(Columns.CountMax, analyticsGrp.DropStats.InternalDrops.Max);
                                dataRow.SetField(Columns.CountMin, analyticsGrp.DropStats.InternalDrops.Min);
                                dataRow.SetField(Columns.CountMean, analyticsGrp.DropStats.InternalDrops.Mean);
                                dataRow.SetField(Columns.CountStdDevp, analyticsGrp.DropStats.InternalDrops.StdDev);
                                dataRow.SetField(Columns.CountTotal, analyticsGrp.DropStats.InternalDrops.Sum);
                            }

                            if (analyticsGrp.DropStats.InternalMeanLatencies.HasValue) // || analyticsGrp.DropStats.CrossNodeDrops.HasValue)
                            {
                                dataRow.SetField(Columns.DurationMax, analyticsGrp.DropStats.InternalMeanLatencies.Max);
                                dataRow.SetField(Columns.DurationMin, analyticsGrp.DropStats.InternalMeanLatencies.Min);
                                dataRow.SetField(Columns.DurationMean, analyticsGrp.DropStats.InternalMeanLatencies.Mean);
                                dataRow.SetField(Columns.DurationStdDevp, analyticsGrp.DropStats.InternalMeanLatencies.StdDev);
                                dataRow.SetField(Columns.DurationTotal, analyticsGrp.DropStats.InternalMeanLatencies.Sum);
                            }

                            unitOfMeasure += analyticsGrp.DropStats.UOM + ',';
                        }

                        if (analyticsGrp.SolrExpiredStats != null && analyticsGrp.SolrExpiredStats.HasValue)
                        {

                            if (analyticsGrp.SolrExpiredStats.NbrFndExpiredDocs.HasValue)
                            {
                                dataRow.SetField(Columns.CountMax, analyticsGrp.SolrExpiredStats.NbrFndExpiredDocs.Max);
                                dataRow.SetField(Columns.CountMin, analyticsGrp.SolrExpiredStats.NbrFndExpiredDocs.Min);
                                dataRow.SetField(Columns.CountMean, analyticsGrp.SolrExpiredStats.NbrFndExpiredDocs.Mean);
                                dataRow.SetField(Columns.CountStdDevp, analyticsGrp.SolrExpiredStats.NbrFndExpiredDocs.StdDev);
                                dataRow.SetField(Columns.CountTotal, analyticsGrp.SolrExpiredStats.NbrFndExpiredDocs.Sum);
                            }

                            if (analyticsGrp.SolrExpiredStats.FndExpiredOPS.HasValue)
                            {
                                dataRow.SetField(Columns.OPSMax, analyticsGrp.SolrExpiredStats.FndExpiredOPS.Max);
                                dataRow.SetField(Columns.OPSMin, analyticsGrp.SolrExpiredStats.FndExpiredOPS.Min);
                                dataRow.SetField(Columns.OPSMean, analyticsGrp.SolrExpiredStats.FndExpiredOPS.Mean);
                                dataRow.SetField(Columns.OPSStdDevp, analyticsGrp.SolrExpiredStats.FndExpiredOPS.StdDev);                                
                            }

                            unitOfMeasure += analyticsGrp.SolrExpiredStats.UOM + ',';
                        }

                        if (analyticsGrp.NodeSyncStats != null && analyticsGrp.NodeSyncStats.HasValue)
                        {

                            if (analyticsGrp.NodeSyncStats.Size.HasValue)
                            {
                                dataRow.SetField(Columns.SizeMax, analyticsGrp.NodeSyncStats.Size.Max);
                                dataRow.SetField(Columns.SizeMin, analyticsGrp.NodeSyncStats.Size.Min);
                                dataRow.SetField(Columns.SizeMean, analyticsGrp.NodeSyncStats.Size.Mean);
                                dataRow.SetField(Columns.SizeStdDevp, analyticsGrp.NodeSyncStats.Size.StdDev);
                                dataRow.SetField(Columns.SizeTotal, analyticsGrp.NodeSyncStats.Size.Sum);
                            }
                            if (analyticsGrp.NodeSyncStats.Rate.HasValue)
                            {
                                dataRow.SetField(Columns.RateMax, analyticsGrp.NodeSyncStats.Rate.Max);
                                dataRow.SetField(Columns.RateMin, analyticsGrp.NodeSyncStats.Rate.Min);
                                dataRow.SetField(Columns.RateMean, analyticsGrp.NodeSyncStats.Rate.Mean);
                                dataRow.SetField(Columns.RateStdDevp, analyticsGrp.NodeSyncStats.Rate.StdDev);
                            }
                            if (analyticsGrp.NodeSyncStats.PercentInconsistent.HasValue)
                            {
                                dataRow.SetField(Columns.EdenThresholdReadRateMax, analyticsGrp.NodeSyncStats.PercentInconsistent.Max);
                                dataRow.SetField(Columns.EdenThresholdReadRateMin, analyticsGrp.NodeSyncStats.PercentInconsistent.Min);
                                dataRow.SetField(Columns.OldThresholdReadRateMean, analyticsGrp.NodeSyncStats.PercentInconsistent.Mean);
                                dataRow.SetField(Columns.OldThresholdReadRateStdDevp, analyticsGrp.NodeSyncStats.PercentInconsistent.StdDev);
                            }

                            unitOfMeasure += analyticsGrp.NodeSyncStats.UOM + ',';
                        }
                    }

                    dataRow.SetField(Columns.UOM, unitOfMeasure.TrimEnd(','));

                    this.Table.Rows.Add(dataRow);
                    ++nbrItems;                    
                }


                #endregion

                Logger.Instance.InfoFormat("Log Aggregation Completed, Total Nbr Items {0:###,###,##0}", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Log Aggregation Canceled");
            }
            finally
            {
                this.Table.AcceptChanges();
                this.Table.EndLoadData();
            }

            return this.Table;
        }
    }
}
