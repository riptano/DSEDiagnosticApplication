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

            dtLog.Columns.Add(ColumnNames.UTCTimeStamp, typeof(DateTime));
            dtLog.Columns.Add(ColumnNames.LogLocalTimeStamp, typeof(DateTime)).AllowDBNull = true;
            dtLog.Columns.Add("Aggregation Period", typeof(TimeSpan));
            dtLog.Columns.Add(ColumnNames.DataCenter, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.KeySpace, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add("Class", typeof(string));
            dtLog.Columns.Add("Related Info", typeof(string));
            dtLog.Columns.Add("HasOrphanedEvents", typeof(bool)).AllowDBNull = true; //Subclass
            dtLog.Columns.Add("Path", typeof(string)); //Exception Path
            dtLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add("Last Occurrence (UTC)", typeof(DateTime));
            dtLog.Columns.Add("Last Occurrence (Local)", typeof(DateTime));
            dtLog.Columns.Add("Occurrences", typeof(long));//n
            dtLog.Columns.Add("UOM", typeof(string));//O
            dtLog.Columns.Add("Duration Max", typeof(decimal)).AllowDBNull = true;//p
            dtLog.Columns.Add("Duration Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Duration Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Duration StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Duration Total", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Rate Max", typeof(decimal)).AllowDBNull = true;//u
            dtLog.Columns.Add("Rate Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Rate Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Rate StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Pending/Tombstone", typeof(long)).AllowDBNull = true;//y
            dtLog.Columns.Add("Completed/Live", typeof(long)).AllowDBNull = true;//z
            dtLog.Columns.Add("Blocked", typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add("AllTimeBlocked", typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add("Eden Max", typeof(decimal)).AllowDBNull = true;//ac
            dtLog.Columns.Add("Eden Dif", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Old Max", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Old Dif", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Survivor Max", typeof(decimal)).AllowDBNull = true;                        
            dtLog.Columns.Add("Survivor Dif", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Size Max", typeof(decimal)).AllowDBNull = true;//ai
            dtLog.Columns.Add("Size Min", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Size Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Size StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Size Total", typeof(decimal)).AllowDBNull = true;//am
            dtLog.Columns.Add("Count Max", typeof(long)).AllowDBNull = true;//an
            dtLog.Columns.Add("Count Min", typeof(long)).AllowDBNull = true;
            dtLog.Columns.Add("Count Mean", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Count StdDevp", typeof(decimal)).AllowDBNull = true;
            dtLog.Columns.Add("Count Total", typeof(long)).AllowDBNull = true;//ar

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

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        public override DataTable LoadTable()
        {
            Logger.Instance.InfoFormat("Log Aggregation Processing Started");

            this.Table.BeginLoadData();
            try
            {               
                var aggPeriods = new List<DateTimeRange>();
               
                #region Determine log Aggregation Period 
                {
                    var logDateRange = new DateTimeOffsetRange();
                    
                    this.Cluster.Nodes.SelectMany(n => n.LogFiles).ForEach(logFile => logDateRange.SetMinMax(logFile.LogDateRange));

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
                var logGrpEvts = from logMMV in this.Cluster.Nodes.SelectMany(n => n.LogEvents)//.AsParallel()
                                 let logEvt = logMMV.GetValue((Common.Patterns.Collections.MemoryMapperElementCreationTypes)LogCassandraEvent.ElementCreationTypes.EventTypeOnly)
                                 where (logEvt.Type & EventTypes.SingleInstance) != 0
                                         || (logEvt.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd
                                         || (logEvt.Type & EventTypes.SessionDefinedByDuration) == EventTypes.SessionDefinedByDuration
                                         || ((logEvt.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin && (logEvt.Class & EventClasses.Orphaned) == EventClasses.Orphaned)
                                 let aggregationDateTime = aggPeriods.First(p => p.Includes(logEvt.EventTime.UtcDateTime))
                                 let logEvent = logMMV.GetValue((Common.Patterns.Collections.MemoryMapperElementCreationTypes)LogCassandraEvent.ElementCreationTypes.AggregationPeriodOnlyWProps)                                 
                                group logEvent by new DSEDiagnosticAnalytics.LogEventGroup(aggregationDateTime,
                                                                                            logEvent)
                                        into g
                                 select new { GroupKey = g.Key,
                                                HasOrphaned = g.Any(e => (e.Class & EventClasses.Orphaned) == EventClasses.Orphaned),
                                                LastEvent = g.OrderBy(e => e.EventTime).Last(),
                                                FirstEvent = g.OrderBy(e => e.EventTime).First(),
                                                NbrOccurs = g.Count(),
                                                AnalyticsGroupings = DSEDiagnosticAnalytics.LogEventGrouping.CreateLogEventGrouping(g.Key, g)};

                DataRow dataRow = null;
                int nbrItems = 0;

                foreach (var logGrpEvt in logGrpEvts
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
                                dataRow.SetField("Pending/Tombstone", analyticsGrp.PoolStats.Pending.Sum);
                            if (analyticsGrp.PoolStats.Completed.HasValue)
                                dataRow.SetField("Completed/Live", analyticsGrp.PoolStats.Completed.Sum);
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
                                dataRow.SetField("Eden Max", Math.Max(analyticsGrp.GCStats.Eden.Before.Max, analyticsGrp.GCStats.Eden.After.Max));
                                dataRow.SetField("Eden Dif", analyticsGrp.GCStats.Eden.Difference.Sum);
                            }
                            if (analyticsGrp.GCStats.Old?.Difference.HasValue ?? false)
                            {
                                dataRow.SetField("Old Max", Math.Max(analyticsGrp.GCStats.Old.Before.Max, analyticsGrp.GCStats.Old.After.Max));
                                dataRow.SetField("Old Dif", analyticsGrp.GCStats.Old.Difference.Sum);
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
                                dataRow.SetField("Pending/Tombstone", analyticsGrp.TombstoneStats.Tombstones.Sum);
                            }
                            if (analyticsGrp.TombstoneStats.LiveCells.HasValue)
                            {
                                dataRow.SetField("Completed/Live", analyticsGrp.TombstoneStats.LiveCells.Sum);
                            }
                            unitOfMeasure += analyticsGrp.TombstoneStats.UOM + ',';
                        }
                        if (analyticsGrp.GossipPendingStats != null && analyticsGrp.GossipPendingStats.HasValue)
                        {
                            if (analyticsGrp.GossipPendingStats.Pending.HasValue)
                            {
                                dataRow.SetField("Pending/Tombstone", analyticsGrp.GossipPendingStats.Pending.Sum);
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
