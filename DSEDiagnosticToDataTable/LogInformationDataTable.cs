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
        public LogInfoDataTable(DSEDiagnosticLibrary.Cluster cluster, IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> logFileStats, CancellationTokenSource cancellationSource = null)
            : base(cluster, cancellationSource)
        {
            this.LogFileStats = logFileStats;
        }

        public IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> LogFileStats { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtLogFile = new DataTable(TableNames.LogInfo, TableNames.Namespace);

            dtLogFile.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)); //a
            dtLogFile.Columns.Add(ColumnNames.DataCenter, typeof(string)).AllowDBNull = true;

            dtLogFile.Columns.Add("Instance Type", typeof(string));//c
            dtLogFile.Columns.Add("Node's TimeZone", typeof(string));//
            dtLogFile.Columns.Add("Time Zone Offset", typeof(string)).AllowDBNull = true; //e
            dtLogFile.Columns.Add("IsDebugLog", typeof(bool));//

            dtLogFile.Columns.Add("Start Range (Used)", typeof(DateTime));//g
            dtLogFile.Columns.Add("End Range (Used)", typeof(DateTime));
            dtLogFile.Columns.Add("Duration (Used)", typeof(TimeSpan));
            dtLogFile.Columns.Add("Start Range (Log File)", typeof(DateTime));//j
            dtLogFile.Columns.Add("End Range (Log File)", typeof(DateTime));
            dtLogFile.Columns.Add("Duration (Log File)", typeof(TimeSpan));
            dtLogFile.Columns.Add("File Path", typeof(string));//m
            dtLogFile.Columns.Add("Events", typeof(long));
            dtLogFile.Columns.Add("File Size (MB)", typeof(decimal));//o
            dtLogFile.Columns.Add("Gap Timespan", typeof(TimeSpan)).AllowDBNull = true;//p
            dtLogFile.Columns.Add("OverLapping", typeof(string)).AllowDBNull = true;//q
            dtLogFile.Columns.Add("Start Range (UTC)", typeof(DateTime)).AllowDBNull = true;
            dtLogFile.Columns.Add("End Range (UTC)", typeof(DateTime)).AllowDBNull = true; //s

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
            DataRow dataRow = null;
            int nbrItems = 0;

            Logger.Instance.InfoFormat("Loading Log Information into DataTable \"{0}\"", this.Table.TableName);

            try
            {
                this.Table.BeginLoadData();

                foreach (var logInfo in from item in this.LogFileStats
                                        let statItem = (DSEDiagnosticAnalytics.LogFileStats.LogInfoStat)item
                                        orderby statItem.Node.Id.NodeName(), statItem.IsDebugFile, statItem.LogRange
                                        select statItem)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    dataRow = this.Table.NewRow();

                    dataRow.SetField(ColumnNames.NodeIPAddress, logInfo.Node.Id.NodeName());
                    dataRow.SetField(ColumnNames.DataCenter, logInfo.DataCenter?.Name);
                    dataRow.SetField("Node's TimeZone", logInfo.Node.Machine.TimeZone?.Name ?? logInfo.Node.Machine.TimeZoneName + '?');
                    dataRow.SetField("Instance Type", logInfo.Product.ToString());
                    dataRow.SetField("IsDebugLog", logInfo.IsDebugFile);
                    dataRow.SetFieldToDecimal("File Size (MB)", logInfo.LogFileSize, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                    dataRow.SetField("OverLapping", logInfo.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Normal ? null : logInfo.OverlappingType.ToString());
                    dataRow.SetField("Start Range (Used)", logInfo.LogRange.Min.DateTime);
                    dataRow.SetField("End Range (Used)", logInfo.LogRange.Max.DateTime);
                    dataRow.SetField("Duration (Used)", logInfo.LogRange.TimeSpan());
                    dataRow.SetFieldToTZOffset("Time Zone Offset", logInfo.LogRange);
                    dataRow.SetField("Start Range (UTC)", logInfo.LogRange.Min.ToUniversalTime().DateTime);
                    dataRow.SetField("End Range (UTC)", logInfo.LogRange.Max.ToUniversalTime().DateTime);
                    dataRow.SetField("Events", logInfo.LogItems);

                    if (logInfo.OverlappingType != DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Continuous)
                    {
                        dataRow.SetField("Start Range (Log File)", logInfo.LogFileRange.Min.DateTime);
                        dataRow.SetField("End Range (Log File)", logInfo.LogFileRange.Max.DateTime);
                        dataRow.SetField("Duration (Log File)", logInfo.LogFileRange.TimeSpan());
                        dataRow.SetField("File Path", logInfo.Path.PathResolved);
                        dataRow.SetField("Gap Timespan", logInfo.Gap);
                    }

                    this.Table.Rows.Add(dataRow);
                    ++nbrItems;
                }
                Logger.Instance.InfoFormat("Loaded Log Information for DataTable \"{0}\", Total Nbr Items {1:###,###,##0}", this.Table.TableName, nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.WarnFormat("Loading Log Information Canceled for Table \"{0}\"", this.Table.TableName);
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
