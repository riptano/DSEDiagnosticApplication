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
        public TimeSpan AggregationPeriod { get; } = Properties.Settings.Default.LogAggregationPeriod;

        public override DataTable CreateInitializationTable()
        {
            var dtLog = new DataTable(TableNames.LogAggregation, TableNames.Namespace);

            if (this.SessionId.HasValue) dtLog.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtLog.Columns.Add(ColumnNames.UTCTimeStamp, typeof(DateTime)); //a
            dtLog.Columns.Add(ColumnNames.LogLocalTimeStamp, typeof(DateTime)).AllowDBNull = true;
            dtLog.Columns.Add("Aggregation Period", typeof(TimeSpan));
            dtLog.Columns.Add(ColumnNames.DataCenter, typeof(string)).AllowDBNull = true;//d
            dtLog.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.KeySpace, typeof(string)).AllowDBNull = true;//f
            dtLog.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add("Class", typeof(string));//h
            dtLog.Columns.Add("Related Info", typeof(string));
            dtLog.Columns.Add("HasOrphanedEvents", typeof(bool)).AllowDBNull = true; //J
            dtLog.Columns.Add("Path", typeof(string)); //Exception Path
            dtLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true; //l

            dtLog.Columns.Add("Last Occurrence (UTC)", typeof(DateTime)); //m
            dtLog.Columns.Add("Last Occurrence (Local)", typeof(DateTime)); //n

            dtLog.Columns.Add("Occurrences", typeof(long));//o

            dtLog.Columns.Add("UOM", typeof(string));//p

            dtLog.Columns.Add("Duration Max", typeof(decimal)).AllowDBNull = true;//q
            dtLog.Columns.Add("Duration Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Duration Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Duration StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Duration Total", typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add("Rate Max", typeof(decimal)).AllowDBNull = true;//v
            dtLog.Columns.Add("Rate Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Rate Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Rate StdDevp", typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add("Pending|Tombstone", typeof(long)).AllowDBNull = true;//z
            dtLog.Columns.Add("Completed|Live", typeof(long)).AllowDBNull = true;//aa
            dtLog.Columns.Add("Blocked", typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add("AllTimeBlocked", typeof(long)).AllowDBNull = true;

            dtLog.Columns.Add("Eden Max|Threshold Max|Read Rate Max", typeof(decimal)).AllowDBNull = true;//ad
            dtLog.Columns.Add("Eden Dif|Threshold Min|Read Rate Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Old Max|Threshold Mean|Read Rate Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Old Dif|Threshold StdDevp|Read Rate StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Survivor Max", typeof(decimal)).AllowDBNull = true;                        
            dtLog.Columns.Add("Survivor Dif", typeof(decimal)).AllowDBNull = true;

            dtLog.Columns.Add("Size Max", typeof(decimal)).AllowDBNull = true;//aj
            dtLog.Columns.Add("Size Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Size Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Size StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Size Total", typeof(decimal)).AllowDBNull = true;//an

            dtLog.Columns.Add("Count Max", typeof(long)).AllowDBNull = true;//ao
            dtLog.Columns.Add("Count Min", typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add("Count Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Count StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Count Total", typeof(long)).AllowDBNull = true;//as

            dtLog.Columns.Add("OPS Max", typeof(long)).AllowDBNull = true;//at
            dtLog.Columns.Add("OPS Min", typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add("OPS Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("OPS StdDevp", typeof(decimal)).AllowDBNull = true; //aw

            dtLog.Columns.Add("Serialized Max", typeof(decimal)).AllowDBNull = true;//ax
            dtLog.Columns.Add("Serialized Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Serialized Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Serialized StdDevp", typeof(decimal)).AllowDBNull = true; //ba
            dtLog.Columns.Add("Serialized Total", typeof(decimal)).AllowDBNull = true;//bb

            dtLog.Columns.Add("Storage Max", typeof(decimal)).AllowDBNull = true;//bc
            dtLog.Columns.Add("Storage Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Storage Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Storage StdDevp", typeof(decimal)).AllowDBNull = true; //bf
            dtLog.Columns.Add("Storage Total", typeof(decimal)).AllowDBNull = true;//bg

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
                var parallelOptions = new System.Threading.Tasks.ParallelOptions();

                parallelOptions.CancellationToken = this.CancellationToken;
                parallelOptions.TaskScheduler = new DSEDiagnosticFileParser.Tasks.Schedulers.WorkStealingTaskScheduler(this.Cluster.Nodes.Count());
               
                System.Threading.Tasks.Parallel.ForEach(this.Cluster.Nodes, parallelOptions, node =>
                {
                    Logger.Instance.InfoFormat("Log Aggregation Processing for {0} Started", node.Id.NodeName());
                    
                    var nodeEvtGrps = (from logMMV in node.LogEventsCache(LogCassandraEvent.ElementCreationTypes.EventTypeOnly, true, false)
                                        let logEvt = logMMV.Value
                                        where (logEvt.Type & EventTypes.SingleInstance) != 0
                                                || (logEvt.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                                                || (logEvt.Type & EventTypes.SessionDefinedByDuration) == EventTypes.SessionDefinedByDuration
                                                || (logEvt.Type & EventTypes.AggregateData) == EventTypes.AggregateData
                                                || ((logEvt.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin && (logEvt.Class & EventClasses.Orphaned) == EventClasses.Orphaned)
                                        let aggregationDateTime = aggPeriods.First(p => p.Includes(logEvt.EventTime.UtcDateTime))
                                        let logEvent = logMMV.GetValue((Common.Patterns.Collections.MemoryMapperElementCreationTypes)LogCassandraEvent.ElementCreationTypes.AggregationPeriodOnlyWProps)
                                        let groupKey = new DSEDiagnosticAnalytics.LogEventGroup(aggregationDateTime, logEvent)
                                        group logEvent by groupKey
                                            into g
                                        let grpEvents = g.OrderBy(e => e.EventTime)
                                        select new LogEvtGroup()
                                        {
                                            GroupKey = g.Key,
                                            HasOrphaned = grpEvents.Any(e => (e.Class & EventClasses.Orphaned) == EventClasses.Orphaned),
                                            LastEvent = grpEvents.Last(),
                                            FirstEvent = grpEvents.First(),
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
                    dataRow.SetField("Aggregation Period", this.AggregationPeriod);
                    dataRow.SetField(ColumnNames.DataCenter, logGrpEvt.GroupKey.DataCenterName);
                    dataRow.SetField(ColumnNames.NodeIPAddress, logGrpEvt.GroupKey.NodeName);
                    dataRow.SetField(ColumnNames.KeySpace, logGrpEvt.GroupKey.KeySpaceName);
                    dataRow.SetField(ColumnNames.Table, logGrpEvt.GroupKey.TableName);
                    dataRow.SetField("Class", logGrpEvt.GroupKey.Class);
                    dataRow.SetField("Related Info", logGrpEvt.GroupKey.AssocItem);
                    if(logGrpEvt.HasOrphaned) dataRow.SetField("HasOrphanedEvents", true);
                    dataRow.SetField("Path", logGrpEvt.GroupKey.Path);
                    dataRow.SetField("Exception", logGrpEvt.GroupKey.Exception);
                    dataRow.SetField("Last Occurrence (UTC)", logGrpEvt.LastEvent.EventTime.UtcDateTime);
                    dataRow.SetField("Last Occurrence (Local)", logGrpEvt.LastEvent.EventTimeLocal);
                    dataRow.SetField("Occurrences", logGrpEvt.NbrOccurs);
                    
                    foreach (var analyticsGrp in logGrpEvt.AnalyticsGroupings)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if (analyticsGrp.DurationStats != null && analyticsGrp.DurationStats.HasValue)
                        {
                            dataRow.SetField("Duration Max", analyticsGrp.DurationStats.Duration.Max);
                            dataRow.SetField("Duration Min", analyticsGrp.DurationStats.Duration.Min);
                            dataRow.SetField("Duration Mean", analyticsGrp.DurationStats.Duration.Mean);
                            dataRow.SetField("Duration StdDevp", analyticsGrp.DurationStats.Duration.StdDev);
                            dataRow.SetField("Duration Total", analyticsGrp.DurationStats.Duration.Sum);
                            unitOfMeasure += analyticsGrp.DurationStats.UOM + ',';
                        }
                        if (analyticsGrp.PoolStats != null && analyticsGrp.PoolStats.HasValue)
                        {
                            if(analyticsGrp.PoolStats.Pending.HasValue)
                                dataRow.SetField("Pending|Tombstone", analyticsGrp.PoolStats.Pending.Sum);
                            if (analyticsGrp.PoolStats.Completed.HasValue)
                                dataRow.SetField("Completed|Live", analyticsGrp.PoolStats.Completed.Sum);
                            if (analyticsGrp.PoolStats.Blocked.HasValue)
                                dataRow.SetField("Blocked", analyticsGrp.PoolStats.Blocked.Sum);
                            if (analyticsGrp.PoolStats.AllTimeBlocked.HasValue)
                                dataRow.SetField("AllTimeBlocked", analyticsGrp.PoolStats.AllTimeBlocked.Sum);
                            unitOfMeasure += analyticsGrp.PoolStats.UOM + ',';
                        }
                        if (analyticsGrp.GCStats != null && analyticsGrp.GCStats.HasValue)
                        {
                            if (analyticsGrp.GCStats.Eden?.Difference.HasValue ?? false)
                            {
                                dataRow.SetField("Eden Max|Threshold Max|Read Rate Max", Math.Max(analyticsGrp.GCStats.Eden.Before.Max, analyticsGrp.GCStats.Eden.After.Max));
                                dataRow.SetField("Eden Dif|Threshold Min|Read Rate Min", analyticsGrp.GCStats.Eden.Difference.Sum);
                            }
                            if (analyticsGrp.GCStats.Old?.Difference.HasValue ?? false)
                            {
                                dataRow.SetField("Old Max|Threshold Mean|Read Rate Mean", Math.Max(analyticsGrp.GCStats.Old.Before.Max, analyticsGrp.GCStats.Old.After.Max));
                                dataRow.SetField("Old Dif|Threshold StdDevp|Read Rate StdDevp", analyticsGrp.GCStats.Old.Difference.Sum);
                            }
                            if (analyticsGrp.GCStats.Survivor?.Difference.HasValue ?? false)
                            {
                                dataRow.SetField("Survivor Max", Math.Max(analyticsGrp.GCStats.Survivor.Before.Max, analyticsGrp.GCStats.Survivor.After.Max));
                                dataRow.SetField("Survivor Dif", analyticsGrp.GCStats.Survivor.Difference.Sum);
                            }
                            unitOfMeasure += analyticsGrp.GCStats.UOM + ',';
                        }
                        if(analyticsGrp.PoolMemTblOPSStats != null && analyticsGrp.PoolMemTblOPSStats.HasValue)
                        {
                            if(analyticsGrp.PoolMemTblOPSStats.OPS.HasValue)
                            {
                                dataRow.SetField("Rate Max", analyticsGrp.PoolMemTblOPSStats.OPS.Max);
                                dataRow.SetField("Rate Min", analyticsGrp.PoolMemTblOPSStats.OPS.Min);
                                dataRow.SetField("Rate Mean", analyticsGrp.PoolMemTblOPSStats.OPS.Mean);
                                dataRow.SetField("Rate StdDevp", analyticsGrp.PoolMemTblOPSStats.OPS.StdDev);
                                //dataRow.SetField("Rate Total", analyticsGrp.PoolMemTblOPSStats.OPS.Sum);
                            }
                            if(analyticsGrp.PoolMemTblOPSStats.Size.HasValue)
                            {
                                dataRow.SetField("Size Max", analyticsGrp.PoolMemTblOPSStats.Size.Max);
                                dataRow.SetField("Size Min", analyticsGrp.PoolMemTblOPSStats.Size.Min);
                                dataRow.SetField("Size Mean", analyticsGrp.PoolMemTblOPSStats.Size.Mean);
                                dataRow.SetField("Size StdDevp", analyticsGrp.PoolMemTblOPSStats.Size.StdDev);
                                dataRow.SetField("Size Total", analyticsGrp.PoolMemTblOPSStats.Size.Sum);
                            }
                            unitOfMeasure += analyticsGrp.PoolMemTblOPSStats.UOM + ',';
                        }
                        if (analyticsGrp.LargePartitionRowStats != null && analyticsGrp.LargePartitionRowStats.HasValue)
                        {                            
                            if (analyticsGrp.LargePartitionRowStats.Size.HasValue)
                            {
                                dataRow.SetField("Size Max", analyticsGrp.LargePartitionRowStats.Size.Max);
                                dataRow.SetField("Size Min", analyticsGrp.LargePartitionRowStats.Size.Min);
                                dataRow.SetField("Size Mean", analyticsGrp.LargePartitionRowStats.Size.Mean);
                                dataRow.SetField("Size StdDevp", analyticsGrp.LargePartitionRowStats.Size.StdDev);
                                dataRow.SetField("Size Total", analyticsGrp.LargePartitionRowStats.Size.Sum);
                            }
                            unitOfMeasure += analyticsGrp.LargePartitionRowStats.UOM + ',';
                        }
                        if (analyticsGrp.TombstoneStats != null && analyticsGrp.TombstoneStats.HasValue)
                        {
                            if (analyticsGrp.TombstoneStats.Tombstones.HasValue)
                            {
                                dataRow.SetField("Pending|Tombstone", analyticsGrp.TombstoneStats.Tombstones.Sum);
                            }
                            if (analyticsGrp.TombstoneStats.LiveCells.HasValue)
                            {
                                dataRow.SetField("Completed|Live", analyticsGrp.TombstoneStats.LiveCells.Sum);
                            }
                            unitOfMeasure += analyticsGrp.TombstoneStats.UOM + ',';
                        }
                        if (analyticsGrp.GossipPendingStats != null && analyticsGrp.GossipPendingStats.HasValue)
                        {
                            if (analyticsGrp.GossipPendingStats.Pending.HasValue)
                            {
                                dataRow.SetField("Pending|Tombstone", analyticsGrp.GossipPendingStats.Pending.Sum);
                            }                            
                            unitOfMeasure += analyticsGrp.GossipPendingStats.UOM + ',';
                        }
                        if (analyticsGrp.BatchSizeStats != null && analyticsGrp.BatchSizeStats.HasValue)
                        {
                            if (analyticsGrp.BatchSizeStats.NbrPartitions.HasValue)
                            {
                                dataRow.SetField("Count Max", analyticsGrp.BatchSizeStats.NbrPartitions.Max);
                                dataRow.SetField("Count Min", analyticsGrp.BatchSizeStats.NbrPartitions.Min);
                                dataRow.SetField("Count Mean", analyticsGrp.BatchSizeStats.NbrPartitions.Mean);
                                dataRow.SetField("Count StdDevp", analyticsGrp.BatchSizeStats.NbrPartitions.StdDev);
                                dataRow.SetField("Count Total", analyticsGrp.BatchSizeStats.NbrPartitions.Sum);
                            }
                            if (analyticsGrp.BatchSizeStats.BatchSizes.HasValue)
                            {
                                dataRow.SetField("Size Max", analyticsGrp.BatchSizeStats.BatchSizes.Max);
                                dataRow.SetField("Size Min", analyticsGrp.BatchSizeStats.BatchSizes.Min);
                                dataRow.SetField("Size Mean", analyticsGrp.BatchSizeStats.BatchSizes.Mean);
                                dataRow.SetField("Size StdDevp", analyticsGrp.BatchSizeStats.BatchSizes.StdDev);
                                dataRow.SetField("Size Total", analyticsGrp.BatchSizeStats.BatchSizes.Sum);
                            }
                            unitOfMeasure += analyticsGrp.BatchSizeStats.UOM + ',';
                        }
                        if (analyticsGrp.PreparesDiscardedStats != null && analyticsGrp.PreparesDiscardedStats.HasValue)
                        {
                            if (analyticsGrp.PreparesDiscardedStats.NbrPrepares.HasValue)
                            {
                                dataRow.SetField("Count Max", analyticsGrp.PreparesDiscardedStats.NbrPrepares.Max);
                                dataRow.SetField("Count Min", analyticsGrp.PreparesDiscardedStats.NbrPrepares.Min);
                                dataRow.SetField("Count Mean", analyticsGrp.PreparesDiscardedStats.NbrPrepares.Mean);
                                dataRow.SetField("Count StdDevp", analyticsGrp.PreparesDiscardedStats.NbrPrepares.StdDev);
                                dataRow.SetField("Count Total", analyticsGrp.PreparesDiscardedStats.NbrPrepares.Sum);
                            }
                            if (analyticsGrp.PreparesDiscardedStats.CacheSizes.HasValue)
                            {
                                dataRow.SetField("Size Max", analyticsGrp.PreparesDiscardedStats.CacheSizes.Max);
                                dataRow.SetField("Size Min", analyticsGrp.PreparesDiscardedStats.CacheSizes.Min);
                                dataRow.SetField("Size Mean", analyticsGrp.PreparesDiscardedStats.CacheSizes.Mean);
                                dataRow.SetField("Size StdDevp", analyticsGrp.PreparesDiscardedStats.CacheSizes.StdDev);
                                dataRow.SetField("Size Total", analyticsGrp.PreparesDiscardedStats.CacheSizes.Sum);
                            }
                            unitOfMeasure += analyticsGrp.PreparesDiscardedStats.UOM + ',';
                        }
                        if (analyticsGrp.FlushStats != null && analyticsGrp.FlushStats.HasValue)
                        {
                            if (analyticsGrp.FlushStats.OPS.HasValue)
                            {
                                dataRow.SetField("OPS Max", analyticsGrp.FlushStats.OPS.Max);
                                dataRow.SetField("OPS Min", analyticsGrp.FlushStats.OPS.Min);
                                dataRow.SetField("OPS Mean", analyticsGrp.FlushStats.OPS.Mean);
                                dataRow.SetField("OPS StdDevp", analyticsGrp.FlushStats.OPS.StdDev);                                
                            }

                            if (analyticsGrp.FlushStats.FlushSize.HasValue)
                            {
                                dataRow.SetField("Size Max", analyticsGrp.FlushStats.FlushSize.Max);
                                dataRow.SetField("Size Min", analyticsGrp.FlushStats.FlushSize.Min);
                                dataRow.SetField("Size Mean", analyticsGrp.FlushStats.FlushSize.Mean);
                                dataRow.SetField("Size StdDevp", analyticsGrp.FlushStats.FlushSize.StdDev);
                                dataRow.SetField("Size Total", analyticsGrp.FlushStats.FlushSize.Sum);
                            }

                            if (analyticsGrp.FlushStats.FlushThreshold.HasValue)
                            {
                                dataRow.SetField("Eden Max|Threshold Max|Read Rate Max", analyticsGrp.FlushStats.FlushThreshold.Max);
                                dataRow.SetField("Eden Dif|Threshold Min|Read Rate Min", analyticsGrp.FlushStats.FlushThreshold.Min);
                                dataRow.SetField("Old Max|Threshold Mean|Read Rate Mean", analyticsGrp.FlushStats.FlushThreshold.Mean);
                                dataRow.SetField("Old Dif|Threshold StdDevp|Read Rate StdDevp", analyticsGrp.FlushStats.FlushThreshold.StdDev);                                
                            }

                            if (analyticsGrp.FlushStats.SerializedSize.HasValue)
                            {
                                dataRow.SetField("Serialized Max", analyticsGrp.FlushStats.SerializedSize.Max);
                                dataRow.SetField("Serialized Min", analyticsGrp.FlushStats.SerializedSize.Min);
                                dataRow.SetField("Serialized Mean", analyticsGrp.FlushStats.SerializedSize.Mean);
                                dataRow.SetField("Serialized StdDevp", analyticsGrp.FlushStats.SerializedSize.StdDev);
                                dataRow.SetField("Serialized Total", analyticsGrp.FlushStats.SerializedSize.Sum);
                            }

                            if (analyticsGrp.FlushStats.StorageSize.HasValue)
                            {
                                dataRow.SetField("Storage Max", analyticsGrp.FlushStats.StorageSize.Max);
                                dataRow.SetField("Storage Min", analyticsGrp.FlushStats.StorageSize.Min);
                                dataRow.SetField("Storage Mean", analyticsGrp.FlushStats.StorageSize.Mean);
                                dataRow.SetField("Storage StdDevp", analyticsGrp.FlushStats.StorageSize.StdDev);
                                dataRow.SetField("Storage Total", analyticsGrp.FlushStats.StorageSize.Sum);
                            }

                            unitOfMeasure += analyticsGrp.FlushStats.UOM + ',';
                        }

                        if (analyticsGrp.CompactionStats != null && analyticsGrp.CompactionStats.HasValue)
                        {
                            if (analyticsGrp.CompactionStats.RowOPS.HasValue)
                            {
                                dataRow.SetField("OPS Max", analyticsGrp.CompactionStats.RowOPS.Max);
                                dataRow.SetField("OPS Min", analyticsGrp.CompactionStats.RowOPS.Min);
                                dataRow.SetField("OPS Mean", analyticsGrp.CompactionStats.RowOPS.Mean);
                                dataRow.SetField("OPS StdDevp", analyticsGrp.CompactionStats.RowOPS.StdDev);
                            }

                            if (analyticsGrp.CompactionStats.NewSize.HasValue)
                            {
                                dataRow.SetField("Size Max", analyticsGrp.CompactionStats.NewSize.Max);
                                dataRow.SetField("Size Min", analyticsGrp.CompactionStats.NewSize.Min);
                                dataRow.SetField("Size Mean", analyticsGrp.CompactionStats.NewSize.Mean);
                                dataRow.SetField("Size StdDevp", analyticsGrp.CompactionStats.NewSize.StdDev);
                                dataRow.SetField("Size Total", analyticsGrp.CompactionStats.NewSize.Sum);
                            }

                            if (analyticsGrp.CompactionStats.OldSize.HasValue)
                            {
                                dataRow.SetField("Storage Max", analyticsGrp.CompactionStats.OldSize.Max);
                                dataRow.SetField("Storage Min", analyticsGrp.CompactionStats.OldSize.Min);
                                dataRow.SetField("Storage Mean", analyticsGrp.CompactionStats.OldSize.Mean);
                                dataRow.SetField("Storage StdDevp", analyticsGrp.CompactionStats.OldSize.StdDev);
                                dataRow.SetField("Storage Total", analyticsGrp.CompactionStats.OldSize.Sum);
                            }

                            if (analyticsGrp.CompactionStats.ReadRate.HasValue)
                            {
                                dataRow.SetField("Eden Max|Threshold Max|Read Rate Max", analyticsGrp.CompactionStats.ReadRate.Max);
                                dataRow.SetField("Eden Dif|Threshold Min|Read Rate Min", analyticsGrp.CompactionStats.ReadRate.Min);
                                dataRow.SetField("Old Max|Threshold Mean|Read Rate Mean", analyticsGrp.CompactionStats.ReadRate.Mean);
                                dataRow.SetField("Old Dif|Threshold StdDevp|Read Rate StdDevp", analyticsGrp.CompactionStats.ReadRate.StdDev);                               
                            }

                            if (analyticsGrp.CompactionStats.WriteRate.HasValue)
                            {
                                dataRow.SetField("Rate Max", analyticsGrp.CompactionStats.WriteRate.Max);
                                dataRow.SetField("Rate Min", analyticsGrp.CompactionStats.WriteRate.Min);
                                dataRow.SetField("Rate Mean", analyticsGrp.CompactionStats.WriteRate.Mean);
                                dataRow.SetField("Rate StdDevp", analyticsGrp.CompactionStats.WriteRate.StdDev);
                            }

                            if (analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.HasValue)
                            {
                                dataRow.SetField("Count Max", analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Max);
                                dataRow.SetField("Count Min", analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Min);
                                dataRow.SetField("Count Mean", analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Mean);
                                dataRow.SetField("Count StdDevp", analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.StdDev);
                                dataRow.SetField("Count Total", analyticsGrp.CompactionStats.PartitionDifferenceMergeCnt.Sum);
                            }

                            unitOfMeasure += analyticsGrp.CompactionStats.UOM + ',';
                        }

                        if (analyticsGrp.HintHandOffStats != null && analyticsGrp.HintHandOffStats.HasValue)
                        {
                            
                            if (analyticsGrp.HintHandOffStats.HintRows.HasValue)
                            {
                                dataRow.SetField("Count Max", analyticsGrp.HintHandOffStats.HintRows.Max);
                                dataRow.SetField("Count Min", analyticsGrp.HintHandOffStats.HintRows.Min);
                                dataRow.SetField("Count Mean", analyticsGrp.HintHandOffStats.HintRows.Mean);
                                dataRow.SetField("Count StdDevp", analyticsGrp.HintHandOffStats.HintRows.StdDev);
                                dataRow.SetField("Count Total", analyticsGrp.HintHandOffStats.HintRows.Sum);
                            }

                            unitOfMeasure += analyticsGrp.HintHandOffStats.UOM + ',';
                        }

                        if (analyticsGrp.MaximumMemoryUsageReachedStats != null && analyticsGrp.MaximumMemoryUsageReachedStats.HasValue)
                        {

                            if (analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.HasValue)
                            {
                                dataRow.SetField("Size Max", analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Max);
                                dataRow.SetField("Size Min", analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Min);
                                dataRow.SetField("Size Mean", analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Mean);
                                dataRow.SetField("Size StdDevp", analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.StdDev);
                                dataRow.SetField("Size Total", analyticsGrp.MaximumMemoryUsageReachedStats.MaxMemory.Sum);
                            }

                            //if (analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.HasValue)
                            //{
                            //    dataRow.SetField("Size Max", analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Max);
                            //    dataRow.SetField("Size Min", analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Min);
                            //    dataRow.SetField("Size Mean", analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Mean);
                            //    dataRow.SetField("Size StdDevp", analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.StdDev);
                            //    dataRow.SetField("Size Total", analyticsGrp.MaximumMemoryUsageReachedStats.ChuckMemory.Sum);
                            //}

                            unitOfMeasure += analyticsGrp.MaximumMemoryUsageReachedStats.UOM + ',';
                        }

                        if (analyticsGrp.FreeDeviceStorageStats != null && analyticsGrp.FreeDeviceStorageStats.HasValue)
                        {
                            if (analyticsGrp.FreeDeviceStorageStats.FreeStorage.HasValue)
                            {
                                dataRow.SetField("Storage Max", analyticsGrp.FreeDeviceStorageStats.FreeStorage.Max);
                                dataRow.SetField("Storage Min", analyticsGrp.FreeDeviceStorageStats.FreeStorage.Min);
                                dataRow.SetField("Storage Mean", analyticsGrp.FreeDeviceStorageStats.FreeStorage.Mean);
                                dataRow.SetField("Storage StdDevp", analyticsGrp.FreeDeviceStorageStats.FreeStorage.StdDev);
                                dataRow.SetField("Storage Total", analyticsGrp.FreeDeviceStorageStats.FreeStorage.Sum);
                            }
                            
                            unitOfMeasure += analyticsGrp.FreeDeviceStorageStats.UOM + ',';
                        }

                        if (analyticsGrp.CompactionInsufficientSpaceStats != null && analyticsGrp.CompactionInsufficientSpaceStats.HasValue)
                        {
                            if (analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.HasValue)
                            {
                                dataRow.SetField("Storage Max", analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Max);
                                dataRow.SetField("Storage Min", analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Min);
                                dataRow.SetField("Storage Mean", analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Mean);
                                dataRow.SetField("Storage StdDevp", analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.StdDev);
                                //dataRow.SetField("Storage Total", analyticsGrp.CompactionInsufficientSpaceStats.RemainingStorage.Sum);
                            }

                            unitOfMeasure += analyticsGrp.CompactionInsufficientSpaceStats.UOM + ',';
                        }

                        if (analyticsGrp.DropStats != null && analyticsGrp.DropStats.HasValue)
                        {

                            if (analyticsGrp.DropStats.InternalDrops.HasValue) // || analyticsGrp.DropStats.CrossNodeDrops.HasValue)
                            {
                                dataRow.SetField("Count Max", analyticsGrp.DropStats.InternalDrops.Max);
                                dataRow.SetField("Count Min", analyticsGrp.DropStats.InternalDrops.Min);
                                dataRow.SetField("Count Mean", analyticsGrp.DropStats.InternalDrops.Mean);
                                dataRow.SetField("Count StdDevp", analyticsGrp.DropStats.InternalDrops.StdDev);
                                dataRow.SetField("Count Total", analyticsGrp.DropStats.InternalDrops.Sum);
                            }

                            if (analyticsGrp.DropStats.InternalMeanLatencies.HasValue) // || analyticsGrp.DropStats.CrossNodeDrops.HasValue)
                            {
                                dataRow.SetField("Duration Max", analyticsGrp.DropStats.InternalMeanLatencies.Max);
                                dataRow.SetField("Duration Min", analyticsGrp.DropStats.InternalMeanLatencies.Min);
                                dataRow.SetField("Duration Mean", analyticsGrp.DropStats.InternalMeanLatencies.Mean);
                                dataRow.SetField("Duration StdDevp", analyticsGrp.DropStats.InternalMeanLatencies.StdDev);
                                dataRow.SetField("Duration Total", analyticsGrp.DropStats.InternalMeanLatencies.Sum);
                            }

                            unitOfMeasure += analyticsGrp.DropStats.UOM + ',';
                        }
                    }

                    dataRow.SetField("UOM", unitOfMeasure.TrimEnd(','));

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
