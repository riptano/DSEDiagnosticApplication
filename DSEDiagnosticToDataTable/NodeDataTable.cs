using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLogger;
using Common;

namespace DSEDiagnosticToDataTable
{
    public sealed class NodeDataTable : DataTableLoad
    {
        public NodeDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
        }
        
        public override DataTable CreateInitializationTable()
        {
            var dtNodeInfo = new DataTable(TableNames.Node, TableNames.Namespace);

            if (this.SessionId.HasValue) dtNodeInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtNodeInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtNodeInfo.Columns[ColumnNames.NodeIPAddress].Unique = true; 
            dtNodeInfo.PrimaryKey = new System.Data.DataColumn[] { dtNodeInfo.Columns[ColumnNames.NodeIPAddress] };

            dtNodeInfo.Columns.Add(ColumnNames.DataCenter, typeof(string));

            dtNodeInfo.Columns.Add("Rack", typeof(string));
            dtNodeInfo.Columns.Add("Status", typeof(string));
            dtNodeInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Cluster Name", typeof(string)).AllowDBNull = true;

            //DataStax Versions
            dtNodeInfo.Columns.Add("DSE", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Cassandra", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Search", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Spark", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Agent", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Schema", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("VNodes", typeof(bool)).AllowDBNull = true;


            dtNodeInfo.Columns.Add("Storage Used (MB)", typeof(decimal)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Storage Utilization", typeof(decimal)).AllowDBNull = true;
            //dtNodeInfo.Columns.Add("Health Rating", typeof(string)).AllowDBNull = true;
            
            dtNodeInfo.Columns.Add("Time Zone Offset", typeof(string)).AllowDBNull = true;

            dtNodeInfo.Columns.Add("Start NodeTool Range", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("End NodeTool Range", typeof(DateTime)).AllowDBNull = true;

            dtNodeInfo.Columns.Add("Uptime", typeof(TimeSpan)).AllowDBNull = true;
            //dtNodeInfo.Columns.Add("Uptime", typeof(string)).AllowDBNull = true;

            dtNodeInfo.Columns.Add("Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Duration", typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Nbr Files", typeof(int)).AllowDBNull = true;

            dtNodeInfo.Columns.Add("Debug Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Debug Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Debug Log Duration", typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Debug Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Debug Log Nbr Files", typeof(int)).AllowDBNull = true;
       
            dtNodeInfo.Columns.Add("Heap Memory (MB)", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Off Heap Memory (MB)", typeof(decimal)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Nbr VNodes", typeof(int)).AllowDBNull = true;
            //dtNodeInfo.Columns.Add("Nbr of Exceptions", typeof(int)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Percent Repaired", typeof(decimal)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Repair Service Enabled", typeof(bool)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Seed Node", typeof(bool)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Gossip Enabled", typeof(bool)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Thrift Enabled", typeof(bool)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Native Transport Enabled", typeof(bool)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Multi-Instance Server Id", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Key Cache Information", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Row Cache Information", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Counter Cache Information", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Chunk Cache Information", typeof(string)).AllowDBNull = true;
            
            dtNodeInfo.DefaultView.ApplyDefaultSort = false;
            dtNodeInfo.DefaultView.AllowDelete = false;
            dtNodeInfo.DefaultView.AllowEdit = false;
            dtNodeInfo.DefaultView.AllowNew = false;
            dtNodeInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC",  ColumnNames.DataCenter, ColumnNames.NodeIPAddress);

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

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);
                        dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                        dataRow.SetField("Rack", node.DSE.Rack);
                        dataRow.SetField("Status", node.DSE.Statuses.ToString());
                        dataRow.SetField("Instance Type", node.DSE.InstanceType.ToString());
                        dataRow.SetField("Cluster Name", node.Cluster.Name);

                        //DataStax Versions
                        dataRow.SetField("DSE", node.DSE.Versions.DSE?.ToString());
                        dataRow.SetField("Cassandra", node.DSE.Versions.Cassandra?.ToString());
                        dataRow.SetField("Search", node.DSE.Versions.Search?.ToString());
                        dataRow.SetField("Spark", node.DSE.Versions.Analytics?.ToString());
                        dataRow.SetField("Agent", node.DSE.Versions.OpsCenterAgent?.ToString());
                        if(node.DSE.Versions.Schema.HasValue)
                            dataRow.SetField("Schema", node.DSE.Versions.Schema.Value.ToString());

                        if (node.DSE.VNodesEnabled.HasValue) dataRow.SetField("VNodes", node.DSE.VNodesEnabled.Value);

                        if(node.Machine.Devices.PercentUtilized == null || node.Machine.Devices.PercentUtilized.IsEmpty())
                        {
                            dataRow.SetFieldToDecimal("Storage Utilization", node.DSE.StorageUtilization, DSEDiagnosticLibrary.UnitOfMeasure.Types.Unknown, true);
                        }
                        else
                        {
                            var percentUtilization = node.Machine.Devices.PercentUtilized
                                                        .Select(i => i.Value)
                                                        .DefaultIfEmpty().Max();

                            if (node.DSE.StorageUtilization.NaN)
                            {
                                if(percentUtilization > 0)
                                    dataRow.SetField("Storage Utilization", percentUtilization);
                            }
                            else
                                dataRow.SetField("Storage Utilization", Math.Max(percentUtilization, node.DSE.StorageUtilization.Value / 100m));

                        }
                        dataRow.SetFieldToDecimal("Storage Used (MB)", node.DSE.StorageUsed, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                        //dataRow.SetField("Health Rating", node.DSE.HealthRating);

                        if (node.DSE.NodeToolDateRange != null)
                        {
                            dataRow.SetField("Start NodeTool Range", node.DSE.NodeToolDateRange.Min.DateTime);
                            dataRow.SetField("End NodeTool Range", node.DSE.NodeToolDateRange.Max.DateTime);

                            dataRow.SetFieldToTZOffset("Time Zone Offset", node.DSE.NodeToolDateRange);
                        }
                        else if(node.DSE.CaptureTimestamp.HasValue)
                        {
                            dataRow.SetField("End NodeTool Range", node.DSE.CaptureTimestamp.Value.DateTime);

                            dataRow.SetFieldToTZOffset("Time Zone Offset", node.DSE.CaptureTimestamp.Value);
                        }

                        dataRow.SetFieldToTimeSpan("Uptime", node.DSE.Uptime);
                                //.SetFieldToTimeSpan("Uptime", node.DSE.Uptime, @"d\ hh\:mm");

                        {
                            TimeSpan minTzOffset = TimeSpan.Zero;
                            TimeSpan maxTZOffset = TimeSpan.Zero;
                            int totalFiles = 0;
                            
                            if (node.DSE.LogSystemDateRange != null)
                            {                               
                                dataRow.SetField("Log Min Timestamp", node.DSE.LogSystemDateRange.Min.DateTime);
                                dataRow.SetField("Log Max Timestamp", node.DSE.LogSystemDateRange.Max.DateTime);
                                dataRow.SetField("Log Duration", node.DSE.LogSystemDuration);
                                dataRow.SetField("Log Timespan Difference", node.DSE.LogSystemGap);

                                minTzOffset = node.DSE.LogSystemDateRange.Min.Offset;
                                maxTZOffset = node.DSE.LogSystemDateRange.Max.Offset;
                            }
                            dataRow.SetField("Log Nbr Files", node.DSE.LogSystemFiles);
                            totalFiles += node.DSE.LogSystemFiles;

                            if (node.DSE.LogDebugDateRange != null)
                            {                                
                                dataRow.SetField("Debug Log Min Timestamp", node.DSE.LogDebugDateRange.Min.DateTime);
                                dataRow.SetField("Debug Log Max Timestamp", node.DSE.LogDebugDateRange.Max.DateTime);
                                dataRow.SetField("Debug Log Duration", node.DSE.LogDebugDuration);
                                dataRow.SetField("Debug Log Timespan Difference", node.DSE.LogDebugGap);

                                if(node.DSE.LogDebugDateRange.Min.Offset != maxTZOffset)
                                    maxTZOffset = node.DSE.LogDebugDateRange.Min.Offset;
                                if (node.DSE.LogDebugDateRange.Max.Offset != minTzOffset)
                                    minTzOffset = node.DSE.LogDebugDateRange.Max.Offset;
                            }
                            dataRow.SetField("Debug Log Nbr Files", node.DSE.LogDebugFiles);
                            totalFiles += node.DSE.LogDebugFiles;

                            if (totalFiles > 0)
                            {
                                dataRow.SetFieldToTZOffset("Time Zone Offset", minTzOffset, maxTZOffset);
                            }
                        }

                        if (!node.DSE.HeapUsed.NaN || !node.DSE.Heap.NaN)
                        {
                            var used = node.DSE.HeapUsed.NaN ? "?" : node.DSE.HeapUsed.ConvertTo(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB).ToString();
                            var size = node.DSE.Heap.NaN ? "?" : node.DSE.Heap.ConvertTo(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB).ToString();
                            dataRow.SetField("Heap Memory (MB)", used + '/' + size);
                        }
                        dataRow.SetFieldToDecimal("Off Heap Memory (MB)", node.DSE.OffHeap, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                        if(node.DSE.NbrTokens.HasValue) dataRow.SetField("Nbr VNodes", (int) node.DSE.NbrTokens.Value);
                        //if(node.DSE.NbrExceptions.HasValue) dataRow.SetField("Nbr of Exceptions", (int) node.DSE.NbrExceptions.Value);
                        dataRow.SetFieldToDecimal("Percent Repaired", node.DSE.RepairedPercent);
                        if(node.DSE.RepairServiceHasRan.HasValue) dataRow.SetField("Repair Service Enabled", node.DSE.RepairServiceHasRan.HasValue);
                        if(node.DSE.GossipEnabled.HasValue) dataRow.SetField("Gossip Enabled", node.DSE.GossipEnabled.Value);
                        if (node.DSE.ThriftEnabled.HasValue) dataRow.SetField("Thrift Enabled", node.DSE.ThriftEnabled.Value);
                        if (node.DSE.NativeTransportEnabled.HasValue) dataRow.SetField("Native Transport Enabled", node.DSE.NativeTransportEnabled.Value);
                        dataRow.SetField("Key Cache Information", node.DSE.KeyCacheInformation);
                        dataRow.SetField("Row Cache Information", node.DSE.RowCacheInformation);
                        dataRow.SetField("Counter Cache Information", node.DSE.CounterCacheInformation);
                        dataRow.SetField("Chunk Cache Information", node.DSE.ChunkCacheInformation);
                        if (node.DSE.IsSeedNode.HasValue) dataRow.SetField("Seed Node", node.DSE.IsSeedNode.Value);
                        if(!string.IsNullOrEmpty(node.DSE.PhysicalServerId)) dataRow.SetField("Multi-Instance Server Id", node.DSE.PhysicalServerId);

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
