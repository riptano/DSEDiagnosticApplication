using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class DSEDevicesDataTable : DataTableLoad
    {
        public DSEDevicesDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        { }

        public override DataTable CreateInitializationTable()
        {
            var dtDSEDeviceInfo = new DataTable(TableNames.DSEDevices, TableNames.Namespace);
            dtDSEDeviceInfo.CaseSensitive();

            if (this.SessionId.HasValue) dtDSEDeviceInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtDSEDeviceInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtDSEDeviceInfo.Columns.Add(ColumnNames.DataCenter, typeof(string));
            
            dtDSEDeviceInfo.Columns.Add("Data", typeof(string)).AllowDBNull = true;
            dtDSEDeviceInfo.Columns.Add("Data Utilization", typeof(decimal)).AllowDBNull = true;
            dtDSEDeviceInfo.Columns.Add("Commit Log", typeof(string)).AllowDBNull = true;
            dtDSEDeviceInfo.Columns.Add("Commit Utilization", typeof(decimal)).AllowDBNull = true;
            dtDSEDeviceInfo.Columns.Add("Saved Cache", typeof(string)).AllowDBNull = true;
            dtDSEDeviceInfo.Columns.Add("Cache Utilization", typeof(decimal)).AllowDBNull = true;
            dtDSEDeviceInfo.Columns.Add("Other", typeof(string)).AllowDBNull = true;
            dtDSEDeviceInfo.Columns.Add("Other Utilization", typeof(decimal)).AllowDBNull = true;
            
            dtDSEDeviceInfo.DefaultView.ApplyDefaultSort = false;
            dtDSEDeviceInfo.DefaultView.AllowDelete = false;
            dtDSEDeviceInfo.DefaultView.AllowEdit = false;
            dtDSEDeviceInfo.DefaultView.AllowNew = false;
            dtDSEDeviceInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [Data] ASC, [Commit Log] ASC, [Saved Cache] ASC, [Other] ASC", ColumnNames.NodeIPAddress, ColumnNames.DataCenter);

            return dtDSEDeviceInfo;
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
                string deviceName = null;
                int nbrItems = 0;

                Logger.Instance.Info("Loading DSE Device Information");

                foreach (var node in this.Cluster.Nodes)
                {                    
                    this.CancellationToken.ThrowIfCancellationRequested();
                    
                    dataRow = this.Table.NewRow();

                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetField(ColumnNames.NodeIPAddress, node.NodeName());
                    dataRow.SetField(ColumnNames.DataCenter, node.DCName());

                    dataRow.SetField("Data", deviceName = node.DSE.Devices.Data?.FirstOrDefault());
                    if(deviceName != null && node.Machine.Devices.PercentUtilized != null) //
                        dataRow.SetField("Data Utilization", node.Machine.Devices.PercentUtilized
                                                                    .Where(i => i.Key.EndsWith('/' + deviceName) || i.Key.EndsWith('/' + deviceName + '1'))
                                                                    .FirstOrDefault().Value);

                    dataRow.SetField("Commit Log", deviceName = node.DSE.Devices.CommitLog);
                    if (deviceName != null && node.Machine.Devices.PercentUtilized != null)
                        dataRow.SetField("Commit Utilization", node.Machine.Devices.PercentUtilized
                                                                    .Where(i => i.Key.EndsWith('/' + deviceName) || i.Key.EndsWith('/' + deviceName + '1'))
                                                                    .FirstOrDefault().Value);

                    dataRow.SetField("Saved Cache", deviceName = node.DSE.Devices.CommitLog);
                    if(deviceName != null && node.Machine.Devices.PercentUtilized != null)
                        dataRow.SetField("Cache Utilization", node.Machine.Devices.PercentUtilized
                                                                    .Where(i => i.Key.EndsWith('/' + deviceName) || i.Key.EndsWith('/' + deviceName + '1'))
                                                                    .FirstOrDefault().Value);

                    dataRow.SetField("Other", deviceName = node.DSE.Devices.Others?.FirstOrDefault());
                    if(deviceName != null && node.Machine.Devices.PercentUtilized != null)
                        dataRow.SetField("Other Utilization", node.Machine.Devices.PercentUtilized
                                                                    .Where(i => i.Key.EndsWith('/' + deviceName) || i.Key.EndsWith('/' + deviceName + '1'))
                                                                    .FirstOrDefault().Value);

                    this.Table.Rows.Add(dataRow);
                    ++nbrItems;

                    var dataCount = node.DSE.Devices.Data?.Count() ?? 0;
                    var otherCount = node.DSE.Devices.Others?.Count() ?? 0;
                    var maxItems = Math.Max(dataCount, otherCount);

                    for(int nIdx = 1; nIdx < maxItems; nIdx++)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow.SetField(ColumnNames.NodeIPAddress, node.NodeName());
                        dataRow.SetField(ColumnNames.DataCenter, node.DCName());

                        dataRow.SetField("Data", deviceName = node.DSE.Devices.Data.ElementAtOrDefault(nIdx));
                        if (deviceName != null && node.Machine.Devices.PercentUtilized != null)
                            dataRow.SetField("Data Utilization", node.Machine.Devices.PercentUtilized
                                                                    .Where(i => i.Key.EndsWith('/' + deviceName) || i.Key.EndsWith('/' + deviceName + '1'))
                                                                    .FirstOrDefault().Value);

                        dataRow.SetField("Other", deviceName = node.DSE.Devices.Others.ElementAtOrDefault(nIdx));
                        if (deviceName != null && node.Machine.Devices.PercentUtilized != null)
                            dataRow.SetField("Other Utilization", node.Machine.Devices.PercentUtilized
                                                                    .Where(i => i.Key.EndsWith('/' + deviceName) || i.Key.EndsWith('/' + deviceName + '1'))
                                                                    .FirstOrDefault().Value);

                        this.Table.Rows.Add(dataRow);
                        ++nbrItems;
                    }
                }

                Logger.Instance.InfoFormat("Loaded DSE Device Information, Total Nbr Items {0:###,###,##0}", nbrItems);

            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading DSE Device Information Canceled");
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
