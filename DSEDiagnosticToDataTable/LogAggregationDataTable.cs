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
            dtLog.Columns.Add("Action", typeof(string)).AllowDBNull = true; //Subclass
            dtLog.Columns.Add("Path", typeof(string)); //Exception Path
            dtLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true;
            dtLog.Columns.Add("Last Occurrence (UTC)", typeof(DateTime));
            dtLog.Columns.Add("Last Occurrence (Local)", typeof(DateTime));
            dtLog.Columns.Add("Occurrences", typeof(long));
            dtLog.Columns.Add("Duration Max", typeof(long));
            dtLog.Columns.Add("Duration Min", typeof(long));
            dtLog.Columns.Add("Duration Mean", typeof(long));
            dtLog.Columns.Add("Duration StdDevp", typeof(long));

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
                var logGrpEvts = from logMMV in this.Cluster.Nodes.SelectMany(n => n.LogEvents)
                                 let logEvt = logMMV.GetValue((Common.Patterns.Collections.MemoryMapperElementCreationTypes)LogCassandraEvent.ElementCreationTypes.EventTypeOnly)
                                 where (logEvt.Type & EventTypes.SingleInstance) != 0
                                         || (logEvt.Type & EventTypes.SessionEnd) != 0
                                         || (logEvt.Type & EventTypes.SessionDefinedByDuration) != 0
                                         || ((logEvt.Type & EventTypes.SessionBegin) != 0 && (logEvt.Class & EventClasses.Orphaned) != 0)
                                 let aggregationDateTime = aggPeriods.First(p => p.Includes(logEvt.EventTime.UtcDateTime))
                                 let logEvent = logMMV.GetValue((Common.Patterns.Collections.MemoryMapperElementCreationTypes)LogCassandraEvent.ElementCreationTypes.AggregationPeriodOnly)
                                 group logEvent by new { AggregationDateTime = aggregationDateTime.Min,
                                     DC = logEvent.DataCenter?.Name,
                                     Node = logEvent.Node?.Id.NodeName(),
                                     KS = logEvent.Keyspace?.Name,
                                     Tbl = logEvent.TableViewIndex?.Name,
                                     Class = (logEvent.Class & ~EventClasses.LogTypes).ToString(),
                                     Action = logEvent.SubClass,
                                     Exception = logEvent.Exception,
                                     Path = logEvent.ExceptionPath?.Join("=>", s => s),
                                 } into g
                                 orderby g.Key.AggregationDateTime ascending,
                                           g.Key.DC,
                                           g.Key.Node,
                                           g.Key.KS,
                                           g.Key.Tbl,
                                           g.Key.Class,
                                           g.Key.Action
                                 let durationLinq = g.Where(e => e.Duration.HasValue).Select(e => e.Duration.Value.TotalMilliseconds)
                                 let hasDuration = durationLinq.HasAtLeastOneElement()
                                 let durationStats = hasDuration ? durationLinq : null
                                 select
                                 new
                                 {
                                     GroupItem = g.Key,
                                     //LogEvents = g,
                                     LastEvent = g.OrderBy(e => e.EventTime.UtcDateTime).FirstOrDefault(),
                                     DurationMax = durationStats == null ? (long?) null : (long?) durationStats.Max(),
                                     DurationMin = durationStats == null ? (long?) null : (long?) durationStats.Min(),
                                     DurationMean = durationStats == null ? (long?) null : (long?) durationStats.Average(),
                                     DurationStdDev = durationStats == null ? (long?) null : (long?) durationStats.StandardDeviationP(),
                                     NbrOccurrs = g.Count()
                                  };

                DataRow dataRow = null;
                int nbrItems = 0;

                foreach (var logGrpEvt in logGrpEvts)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    dataRow = this.Table.NewRow();

                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetField(ColumnNames.UTCTimeStamp, logGrpEvt.GroupItem.AggregationDateTime);
                    dataRow.SetField(ColumnNames.LogLocalTimeStamp, logGrpEvt.GroupItem.AggregationDateTime + logGrpEvt.LastEvent.EventTime.Offset);
                    dataRow.SetField("Aggregation Period", this.AggregationPeriod);
                    dataRow.SetField(ColumnNames.DataCenter, logGrpEvt.GroupItem.DC);
                    dataRow.SetField(ColumnNames.NodeIPAddress, logGrpEvt.GroupItem.Node);
                    dataRow.SetField(ColumnNames.KeySpace, logGrpEvt.GroupItem.KS);
                    dataRow.SetField(ColumnNames.Table, logGrpEvt.GroupItem.Tbl);
                    dataRow.SetField("Class", logGrpEvt.GroupItem.Class);
                    dataRow.SetField("Action", logGrpEvt.GroupItem.Action);
                    dataRow.SetField("Path", logGrpEvt.GroupItem.Path);
                    dataRow.SetField("Exception", logGrpEvt.GroupItem.Exception);
                    dataRow.SetField("Last Occurrence (UTC)", logGrpEvt.LastEvent.EventTime.UtcDateTime);
                    dataRow.SetField("Last Occurrence (Local)", logGrpEvt.LastEvent.EventTimeLocal);
                    dataRow.SetField("Occurrences", logGrpEvt.NbrOccurrs);

                    if(logGrpEvt.DurationMax.HasValue)
                    {
                        dataRow.SetField("Duration Max", logGrpEvt.DurationMax);
                        dataRow.SetField("Duration Min", logGrpEvt.DurationMin);
                        dataRow.SetField("Duration Mean", logGrpEvt.DurationMean);
                        dataRow.SetField("Duration StdDevp", logGrpEvt.DurationStdDev);
                    }

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
