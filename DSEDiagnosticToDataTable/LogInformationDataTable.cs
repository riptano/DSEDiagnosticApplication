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
            dtLogFile.Columns.Add("Start Range (Used)", typeof(DateTime));//e
            dtLogFile.Columns.Add("End Range (Used)", typeof(DateTime));
            dtLogFile.Columns.Add("Duration (Used)", typeof(TimeSpan));
            dtLogFile.Columns.Add("Start Range (Log File)", typeof(DateTime));//h
            dtLogFile.Columns.Add("End Range (Log File)", typeof(DateTime));
            dtLogFile.Columns.Add("Duration (Log File)", typeof(TimeSpan));
            dtLogFile.Columns.Add("File Path", typeof(string));//K
            dtLogFile.Columns.Add("Events", typeof(long));
            dtLogFile.Columns.Add("File Size (MB)", typeof(decimal));//m
            dtLogFile.Columns.Add("IsDebugLog", typeof(bool));//n

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
                int nbrItems = 0;

                foreach (var node in this.Cluster.Nodes
                                        .OrderBy(n => n.Id.NodeName()))
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    nbrItems = 0;
                    Logger.Instance.InfoFormat("Loading Log Information for Node \"{0}\"", node);

                    foreach (var logInfo in node.LogFiles.OrderBy(l => l.LogDateRange))
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();
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
                        dataRow.SetField("IsDebugLog", logInfo.LogFile.Name.IndexOf("debug", StringComparison.OrdinalIgnoreCase) >= 0);
                        this.Table.Rows.Add(dataRow);
                        ++nbrItems;
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
