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
        public NodeDataTable(DSEDiagnosticLibrary.Cluster cluster, IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> logFileStats, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.LogFileStats = logFileStats;
        }

        public IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> LogFileStats { get; }


        public override DataTable CreateInitializationTable()
        {
            var dtNodeInfo = new DataTable(TableNames.Node, TableNames.Namespace);

            if (this.SessionId.HasValue) dtNodeInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtNodeInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtNodeInfo.Columns[ColumnNames.NodeIPAddress].Unique = true; //A
            dtNodeInfo.PrimaryKey = new System.Data.DataColumn[] { dtNodeInfo.Columns[ColumnNames.NodeIPAddress] };

            dtNodeInfo.Columns.Add(ColumnNames.DataCenter, typeof(string));

            dtNodeInfo.Columns.Add("Rack", typeof(string));
            dtNodeInfo.Columns.Add("Status", typeof(string));
            dtNodeInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Cluster Name", typeof(string)).AllowDBNull = true; //F

            //DataStax Versions
            dtNodeInfo.Columns.Add("DSE", typeof(string)).AllowDBNull = true; //G
            dtNodeInfo.Columns.Add("Cassandra", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Search", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Spark", typeof(string)).AllowDBNull = true;//J
            dtNodeInfo.Columns.Add("Agent", typeof(string)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("VNodes", typeof(bool)).AllowDBNull = true; //L


            dtNodeInfo.Columns.Add("Storage Used (MB)", typeof(decimal)).AllowDBNull = true; //M
            dtNodeInfo.Columns.Add("Storage Utilization", typeof(decimal)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Health Rating", typeof(string)).AllowDBNull = true;

            dtNodeInfo.Columns.Add("Time Zone Offset", typeof(string)).AllowDBNull = true; //P

            dtNodeInfo.Columns.Add("Start NodeTool Range", typeof(DateTime)).AllowDBNull = true; //Q
            dtNodeInfo.Columns.Add("End NodeTool Range", typeof(DateTime)).AllowDBNull = true; //R

            dtNodeInfo.Columns.Add("Uptime", typeof(TimeSpan)).AllowDBNull = true; //S
            //dtNodeInfo.Columns.Add("Uptime", typeof(string)).AllowDBNull = true;

            dtNodeInfo.Columns.Add("Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;//T
            dtNodeInfo.Columns.Add("Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Duration", typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;//W
            dtNodeInfo.Columns.Add("Log Nbr Files", typeof(int)).AllowDBNull = true;//X

            dtNodeInfo.Columns.Add("Debug Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;//Y
            dtNodeInfo.Columns.Add("Debug Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Debug Log Duration", typeof(TimeSpan)).AllowDBNull = true;//AA
            dtNodeInfo.Columns.Add("Debug Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;//ab
            dtNodeInfo.Columns.Add("Debug Log Nbr Files", typeof(int)).AllowDBNull = true;//ac

            dtNodeInfo.Columns.Add("Heap Memory (MB)", typeof(string)).AllowDBNull = true; //ad
            dtNodeInfo.Columns.Add("Off Heap Memory (MB)", typeof(decimal)).AllowDBNull = true;//ae
            dtNodeInfo.Columns.Add("Nbr VNodes", typeof(int)).AllowDBNull = true;//af
            dtNodeInfo.Columns.Add("Nbr of Exceptions", typeof(int)).AllowDBNull = true;//ag
            dtNodeInfo.Columns.Add("Percent Repaired", typeof(decimal)).AllowDBNull = true;//ah
            dtNodeInfo.Columns.Add("Repair Service Enabled", typeof(bool)).AllowDBNull = true;//ai
            dtNodeInfo.Columns.Add("Seed Node", typeof(bool)).AllowDBNull = true;//aj
            dtNodeInfo.Columns.Add("Gossip Enabled", typeof(bool)).AllowDBNull = true;//ak
            dtNodeInfo.Columns.Add("Thrift Enabled", typeof(bool)).AllowDBNull = true;//al
            dtNodeInfo.Columns.Add("Native Transport Enabled", typeof(bool)).AllowDBNull = true;//am
            dtNodeInfo.Columns.Add("Multi-Instance Server Id", typeof(string)).AllowDBNull = true;//an
            dtNodeInfo.Columns.Add("Key Cache Information", typeof(string)).AllowDBNull = true;//ao
            dtNodeInfo.Columns.Add("Row Cache Information", typeof(string)).AllowDBNull = true;//ap
            dtNodeInfo.Columns.Add("Counter Cache Information", typeof(string)).AllowDBNull = true;//aq
            dtNodeInfo.Columns.Add("Chunk Cache Information", typeof(string)).AllowDBNull = true;//ar
            
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
                        if (node.DSE.VNodesEnabled.HasValue) dataRow.SetField("VNodes", node.DSE.VNodesEnabled.Value);

                        dataRow.SetFieldToDecimal("Storage Used (MB)", node.DSE.StorageUsed, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                .SetFieldToDecimal("Storage Utilization", node.DSE.StorageUtilization, DSEDiagnosticLibrary.UnitOfMeasure.Types.Unknown, true);
                        dataRow.SetField("Health Rating", node.DSE.HealthRating);

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
                            int fileCnt;
                            int totalFiles = 0;
                            var nodeFileLogInfo = this.LogFileStats.Where(l => l.Node == node).Cast<DSEDiagnosticAnalytics.LogFileStats.LogInfoStat>();
                            var systemLogFiles = nodeFileLogInfo
                                                       .Where(l => !l.IsDebugFile);                                                       
                            var debugLogFiles = nodeFileLogInfo
                                                        .Where(l => l.IsDebugFile);

                            var systemLogEntries = systemLogFiles;

                            if (systemLogEntries.HasAtLeastOneElement())
                            {
                                var systemMaxLogTS = systemLogEntries.Max(l => l.LogRange.Max);
                                var systemMinLogTS = systemLogEntries.Min(l => l.LogRange.Min);
                                var systemDuration = TimeSpan.FromSeconds(systemLogEntries
                                                                            .Where(l=> l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Normal)
                                                                            .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                            .DefaultIfEmpty().Sum());
                                var systemGap = TimeSpan.FromSeconds(systemLogEntries
                                                                        .Where(l => l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Gap)
                                                                        .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                        .DefaultIfEmpty().Sum());

                                dataRow.SetField("Log Min Timestamp", systemMinLogTS.DateTime);
                                dataRow.SetField("Log Max Timestamp", systemMaxLogTS.DateTime);
                                dataRow.SetField("Log Duration", systemDuration);
                                dataRow.SetField("Log Timespan Difference", systemGap);

                                minTzOffset = systemMinLogTS.Offset;
                                maxTZOffset = systemMaxLogTS.Offset;
                            }
                            dataRow.SetField("Log Nbr Files", fileCnt = systemLogEntries.Count());
                            totalFiles += fileCnt;

                            var debugLogEntries = debugLogFiles;

                            if (debugLogEntries.HasAtLeastOneElement())
                            {
                                var debugMaxLogTS = debugLogEntries.Max(l => l.LogRange.Max);
                                var debugMinLogTS = debugLogEntries.Min(l => l.LogRange.Min);
                                var debugDuration = TimeSpan.FromSeconds(debugLogEntries
                                                                            .Where(l => l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Normal)
                                                                            .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                            .DefaultIfEmpty().Sum());
                                var debugGap = TimeSpan.FromSeconds(debugLogEntries
                                                                        .Where(l => l.OverlappingType == DSEDiagnosticAnalytics.LogFileStats.LogInfoStat.OverLappingTypes.Gap)
                                                                        .Select(l => l.LogRange.TimeSpan().TotalSeconds)
                                                                        .DefaultIfEmpty().Sum());

                                dataRow.SetField("Debug Log Min Timestamp", debugMinLogTS.DateTime);
                                dataRow.SetField("Debug Log Max Timestamp", debugMaxLogTS.DateTime);
                                dataRow.SetField("Debug Log Duration", debugDuration);
                                dataRow.SetField("Debug Log Timespan Difference", debugGap);

                                if(debugMaxLogTS.Offset != maxTZOffset)
                                    maxTZOffset = debugMaxLogTS.Offset;
                                if (debugMinLogTS.Offset != minTzOffset)
                                    minTzOffset = debugMinLogTS.Offset;

                            }
                            dataRow.SetField("Debug Log Nbr Files", fileCnt = debugLogEntries.Count());
                            totalFiles += fileCnt;

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
                        if(node.DSE.NbrExceptions.HasValue) dataRow.SetField("Nbr of Exceptions", (int) node.DSE.NbrExceptions.Value);
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
