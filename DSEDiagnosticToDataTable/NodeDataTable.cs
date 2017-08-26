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
    public sealed class NodeDataTable : DataTableLoad
    {
        public NodeDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null)
            : base(cluster, cancellationSource)
        {}

        public override DataTable CreateInitializationTable()
        {
            var dtNodeInfo = new DataTable(TableNames.Node, TableNames.Namespace);

            dtNodeInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtNodeInfo.Columns[ColumnNames.NodeIPAddress].Unique = true; //A
            dtNodeInfo.PrimaryKey = new System.Data.DataColumn[] { dtNodeInfo.Columns[ColumnNames.NodeIPAddress] };

            dtNodeInfo.Columns.Add(ColumnNames.DataCenter, typeof(string));

            dtNodeInfo.Columns.Add("Rack", typeof(string));
            dtNodeInfo.Columns.Add("Status", typeof(string));
            dtNodeInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Cluster Name", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Storage Used (MB)", typeof(decimal)).AllowDBNull = true; //G
            dtNodeInfo.Columns.Add("Storage Utilization", typeof(decimal)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Health Rating", typeof(string)).AllowDBNull = true;

            dtNodeInfo.Columns.Add("Uptime (Days)", typeof(TimeSpan)).AllowDBNull = true; //J
            dtNodeInfo.Columns.Add("Uptime", typeof(string)).AllowDBNull = true; //K

            dtNodeInfo.Columns.Add("Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;//L
            dtNodeInfo.Columns.Add("Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Duration", typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;//O

            dtNodeInfo.Columns.Add("Debug Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;//P
            dtNodeInfo.Columns.Add("Debug Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Debug Log Duration", typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Debug Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;//s

            dtNodeInfo.Columns.Add("Heap Memory (MB)", typeof(string)).AllowDBNull = true; //t
            dtNodeInfo.Columns.Add("Off Heap Memory (MB)", typeof(decimal)).AllowDBNull = true;//u
            dtNodeInfo.Columns.Add("Nbr VNodes", typeof(int)).AllowDBNull = true;//v
            dtNodeInfo.Columns.Add("Nbr of Exceptions", typeof(int)).AllowDBNull = true;//w
            dtNodeInfo.Columns.Add("Percent Repaired", typeof(decimal)).AllowDBNull = true;//x
            dtNodeInfo.Columns.Add("Repair Service Enabled", typeof(bool)).AllowDBNull = true;//y
            dtNodeInfo.Columns.Add("Gossip Enabled", typeof(bool)).AllowDBNull = true;//z
            dtNodeInfo.Columns.Add("Thrift Enabled", typeof(bool)).AllowDBNull = true;//aa
            dtNodeInfo.Columns.Add("Native Transport Enabled", typeof(bool)).AllowDBNull = true;//ab
            dtNodeInfo.Columns.Add("Key Cache Information", typeof(string)).AllowDBNull = true;//ac
            dtNodeInfo.Columns.Add("Row Cache Information", typeof(string)).AllowDBNull = true;//ad
            dtNodeInfo.Columns.Add("Counter Cache Information", typeof(string)).AllowDBNull = true;//ae

            dtNodeInfo.DefaultView.ApplyDefaultSort = false;
            dtNodeInfo.DefaultView.AllowDelete = false;
            dtNodeInfo.DefaultView.AllowEdit = false;
            dtNodeInfo.DefaultView.AllowNew = false;
            dtNodeInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC", ColumnNames.NodeIPAddress, ColumnNames.DataCenter);

            return dtNodeInfo;
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

                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading Node Information for DC \"{0}\"", dataCenter.Name);

                    foreach (var node in dataCenter.Nodes)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);
                        dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                        dataRow.SetField("Rack", node.DSE.Rack);
                        dataRow.SetField("Status", node.DSE.Statuses.ToString());
                        dataRow.SetField("Instance Type", node.DSE.InstanceType.ToString());
                        dataRow.SetField("Cluster Name", node.Cluster.Name);
                        dataRow.SetFieldToDecimal("Storage Used (MB)", node.DSE.StorageUsed, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                .SetFieldToDecimal("Storage Utilization", node.DSE.StorageUtilization, DSEDiagnosticLibrary.UnitOfMeasure.Types.Unknown, true);
                        dataRow.SetField("Health Rating", node.DSE.HealthRating);

                        dataRow.SetFieldToTimeSpan("Uptime (Days)", node.DSE.Uptime)
                                .SetFieldToTimeSpan("Uptime", node.DSE.Uptime, @"d\ hh\:mm");

                        /*
                        dataRow.SetField("Log Min Timestamp", typeof(DateTime));
                        dataRow.SetField("Log Max Timestamp", typeof(DateTime));
                        dataRow.SetField("Log Duration", typeof(TimeSpan));
                        dataRow.SetField("Log Timespan Difference", typeof(TimeSpan));

                        dataRow.SetField("Debug Log Min Timestamp", typeof(DateTime));
                        dataRow.SetField("Debug Log Max Timestamp", typeof(DateTime));
                        dataRow.SetField("Debug Log Duration", typeof(TimeSpan));
                        dataRow.SetField("Debug Log Timespan Difference", typeof(TimeSpan))
                        */

                        if(node.DSE.HeapUsed != null || node.DSE.Heap != null)
                        {
                            var used = node.DSE.HeapUsed == null || node.DSE.HeapUsed.NaN ? "?" : node.DSE.HeapUsed.ConvertTo(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB).ToString();
                            var size = node.DSE.Heap == null || node.DSE.Heap.NaN ? "?" : node.DSE.Heap.ConvertTo(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB).ToString();
                            dataRow.SetField("Heap Memory (MB)", used + '/' + size);
                        }
                        dataRow.SetFieldToDecimal("Off Heap Memory (MB)", node.DSE.OffHeap, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                        if(node.DSE.NbrTokens.HasValue) dataRow.SetField("Nbr VNodes", (int) node.DSE.NbrTokens.Value);
                        if(node.DSE.NbrExceptions.HasValue) dataRow.SetField("Nbr of Exceptions", (int) node.DSE.NbrExceptions.Value);
                        //dataRow.SetField("Percent Repaired", typeof(decimal));
                        if(node.DSE.RepairServiceHasRan.HasValue) dataRow.SetField("Repair Service Enabled", node.DSE.RepairServiceHasRan.HasValue);
                        if(node.DSE.GossipEnabled.HasValue) dataRow.SetField("Gossip Enabled", node.DSE.GossipEnabled.Value);
                        if (node.DSE.ThriftEnabled.HasValue) dataRow.SetField("Thrift Enabled", node.DSE.ThriftEnabled.Value);
                        if (node.DSE.NativeTransportEnabled.HasValue) dataRow.SetField("Native Transport Enabled", node.DSE.NativeTransportEnabled.Value);
                        dataRow.SetField("Key Cache Information", node.DSE.KeyCacheInformation);
                        dataRow.SetField("Row Cache Information", node.DSE.RowCacheInformation);
                        dataRow.SetField("Counter Cache Information", node.DSE.CounterCacheInformation);

                        this.Table.Rows.Add(dataRow);
                        ++nbrItems;
                    }

                    Logger.Instance.InfoFormat("Loaded Node Information for DC \"{0}\", Total Nbr Items {1:###,###,##0}", dataCenter.Name, nbrItems);
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Node Information Canceled");
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
