using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class LogInfoDataTable : DataTableLoad
    {
        public LogInfoDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null)
            : base(cluster, cancellationSource)
        {
        }

        public override DataTable CreateInitializationTable()
        {
            var dtLogFile = new DataTable(TableNames.LogInfo, TableNames.Namespace);

            dtLogFile.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)); //a
            dtLogFile.Columns.Add(ColumnNames.DataCenter, typeof(string)).AllowDBNull = true;

            dtLogFile.Columns.Add("Instance Type", typeof(string));//c
            dtLogFile.Columns.Add("Node's TimeZone", typeof(string));//
            dtLogFile.Columns.Add("IsDebugLog", typeof(bool));//
            dtLogFile.Columns.Add("Start Range (Used)", typeof(DateTimeOffset));//f
            dtLogFile.Columns.Add("End Range (Used)", typeof(DateTimeOffset));
            dtLogFile.Columns.Add("Duration (Used)", typeof(TimeSpan));
            dtLogFile.Columns.Add("Start Range (Log File)", typeof(DateTimeOffset));//i
            dtLogFile.Columns.Add("End Range (Log File)", typeof(DateTimeOffset));
            dtLogFile.Columns.Add("Duration (Log File)", typeof(TimeSpan));
            dtLogFile.Columns.Add("File Path", typeof(string));//l
            dtLogFile.Columns.Add("Events", typeof(long));
            dtLogFile.Columns.Add("File Size (MB)", typeof(decimal));//n
            dtLogFile.Columns.Add("Gap Timespan", typeof(TimeSpan)).AllowDBNull = true;//o
            dtLogFile.Columns.Add("OverLapping", typeof(string)).AllowDBNull = true;//p
            dtLogFile.Columns.Add("Start Range (UTC)", typeof(DateTime)).AllowDBNull = true;
            dtLogFile.Columns.Add("End Range (UTC)", typeof(DateTime)).AllowDBNull = true;
            dtLogFile.Columns.Add("Start Continuous (UTC)", typeof(DateTime)).AllowDBNull = true;//s
            dtLogFile.Columns.Add("End Continuous (UTC)", typeof(DateTime)).AllowDBNull = true;
            dtLogFile.Columns.Add("Continuous Duration", typeof(TimeSpan)).AllowDBNull = true;

            dtLogFile.DefaultView.ApplyDefaultSort = false;
            dtLogFile.DefaultView.AllowDelete = false;
            dtLogFile.DefaultView.AllowEdit = false;
            dtLogFile.DefaultView.AllowNew = false;
           // dtLogFile.DefaultView.Sort = string.Format("[Name] ASC, [{0}] ASC", ColumnNames.DataCenter);

            return dtLogFile;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        public override DataTable LoadTable()
        {
            this.Table.BeginLoadData();
            try
            {
                DataRow dataRow = null;
                DataRow lastContDataRow = null;
                TimeSpan? gaptimeSpan = null;
                DateTimeOffsetRange contTimeRange = null;
                TimeSpan contTimeSpan = new TimeSpan();
                int nbrItems = 0;

                foreach (var node in this.Cluster.Nodes
                                        .OrderBy(n => n.Id.NodeName()))
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    nbrItems = 0;
                    Logger.Instance.InfoFormat("Loading Log Information for Node \"{0}\"", node);
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

                            dataRow = this.Table.NewRow();

                            if (nIdx > 0)
                            {
                                if (logInfo.LogDateRange == fileType.LogInfos[nIdx - 1].LogDateRange || fileType.LogInfos[nIdx - 1].LogDateRange.IsWithIn(logInfo.LogDateRange.Min))
                                {
                                    dataRow.SetField("OverLapping", "Duplicated");
                                    gaptimeSpan = null;
                                }
                                else if (fileType.LogInfos[nIdx - 1].LogDateRange.Includes(logInfo.LogDateRange.Min))
                                {
                                    dataRow.SetField("OverLapping", "Overlapping");
                                    gaptimeSpan = null;
                                }
                                else
                                {
                                    dataRow.SetField("Gap Timespan", gaptimeSpan = (TimeSpan?)(logInfo.LogDateRange.Min - fileType.LogInfos[nIdx - 1].LogDateRange.Max));
                                }
                            }
                            else
                            {
                                gaptimeSpan = TimeSpan.Zero;
                                contTimeRange = new DateTimeOffsetRange();
                                contTimeSpan = new TimeSpan();
                                lastContDataRow = null;
                            }

                            dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                            dataRow.SetField(ColumnNames.DataCenter, node.DataCenter?.Name);
                            dataRow.SetField("Node's TimeZone", node.Machine.TimeZone?.Name ?? node.Machine.TimeZoneName + '?');
                            dataRow.SetField("Instance Type", node.DSE.InstanceType.ToString());
                            dataRow.SetField("Start Range (Log File)", logInfo.LogFileDateRange.Min);
                            dataRow.SetField("End Range (Log File)", logInfo.LogFileDateRange.Max);
                            dataRow.SetField("Duration (Log File)", logInfo.LogFileDateRange.TimeSpan());
                            dataRow.SetField("Start Range (Used)", logInfo.LogDateRange.Min);
                            dataRow.SetField("End Range (Used)", logInfo.LogDateRange.Max);
                            dataRow.SetField("Duration (Used)", logInfo.LogDateRange.TimeSpan());
                            dataRow.SetField("File Path", logInfo.LogFile.PathResolved);
                            dataRow.SetField("Events", logInfo.LogItems);
                            dataRow.SetFieldToDecimal("File Size (MB)", logInfo.LogFileSize, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                            dataRow.SetField("IsDebugLog", fileType.IsDebugFile);

                            dataRow.SetField("Start Range (UTC)", logInfo.LogFileDateRange.Min.ToUniversalTime().DateTime);
                            dataRow.SetField("End Range (UTC)", logInfo.LogFileDateRange.Max.ToUniversalTime().DateTime);                            

                            if(gaptimeSpan.HasValue)
                            {
                                if (Math.Abs(gaptimeSpan.Value.TotalMinutes) <= Properties.Settings.Default.LogFileInfoAnalysisGapTriggerInMins)
                                {
                                    contTimeSpan = contTimeSpan.Add(logInfo.LogDateRange.TimeSpan());
                                    contTimeRange.SetMinimal(logInfo.LogFileDateRange.Min.ToUniversalTime().DateTime);
                                    contTimeRange.SetMaximum(logInfo.LogFileDateRange.Max.ToUniversalTime().DateTime);
                                    lastContDataRow = dataRow;
                                }
                                else
                                {
                                    if(lastContDataRow != null && contTimeSpan.TotalDays >= Properties.Settings.Default.LogFileInfoAnalysisContinousEventInDays)
                                    {
                                        lastContDataRow.SetField("Start Continuous (UTC)", contTimeRange.Min.DateTime);
                                        lastContDataRow.SetField("End Continuous (UTC)", contTimeRange.Max.DateTime);
                                        lastContDataRow.SetField("Continuous Duration", contTimeSpan);
                                    }

                                    contTimeRange = new DateTimeOffsetRange(logInfo.LogFileDateRange.Min.ToUniversalTime(), logInfo.LogFileDateRange.Max.ToUniversalTime());
                                    contTimeSpan = new TimeSpan(logInfo.LogDateRange.TimeSpan().Ticks);
                                    lastContDataRow = dataRow;
                                }
                            }


                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;
                        }

                        if (lastContDataRow != null && contTimeSpan.TotalDays >= Properties.Settings.Default.LogFileInfoAnalysisContinousEventInDays)
                        {
                            lastContDataRow.SetField("Start Continuous (UTC)", contTimeRange.Min.DateTime);
                            lastContDataRow.SetField("End Continuous (UTC)", contTimeRange.Max.DateTime);
                            lastContDataRow.SetField("Continuous Duration", contTimeSpan);
                        }
                    }

                    Logger.Instance.InfoFormat("Loaded Log Information for Node \"{0}\", Total Nbr Items {1:###,###,##0}", node, nbrItems);
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Log Information Canceled");
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
