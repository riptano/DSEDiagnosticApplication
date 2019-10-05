using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLibrary;
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
          
            dtNodeInfo.Columns.Add("Status Starts", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Status Restarts", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Long Down Pause", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Status Not Responding", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Status Dead", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Network Events", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Status Other", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Device Utilized", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("DU Insufficient Space", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Storage Total", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Storage Total (User)", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Storage Percent", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("SSTables Total", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("SSTables Total (User)", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("SSTables Percent", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Reads Total", typeof(ulong))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Reads Total (User)", typeof(ulong))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Reads Percent", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Writes Total", typeof(ulong))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Writes Total (User)", typeof(ulong))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Writes Percent", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Batch Percent", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("LWT Percent", typeof(decimal))
                            .AllowDBNull();

            dtNodeInfo.Columns.Add("Log Event Total", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Log Event Percent", typeof(decimal))
                            .AllowDBNull();

            dtNodeInfo.Columns.Add("GC Events Total", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("GC Events Percent", typeof(decimal))
                            .AllowDBNull();

            dtNodeInfo.Columns.Add("Flush Total", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Flush Total (User)", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Flush Percent", typeof(decimal))
                            .AllowDBNull();

            dtNodeInfo.Columns.Add("Compaction Total", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Compaction Total (User)", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Compaction Percent", typeof(decimal))
                            .AllowDBNull();

            dtNodeInfo.Columns.Add("Repair Total", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Repair Total (User)", typeof(long))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Repair Percent", typeof(decimal))
                            .AllowDBNull();
            dtNodeInfo.Columns.Add("Repair Est Tasks (User)", typeof(long))
                           .AllowDBNull();
            dtNodeInfo.Columns.Add("Node Sync Tables (User)", typeof(long))
                          .AllowDBNull();
            dtNodeInfo.Columns.Add("Node Sync Count", typeof(long))
                          .AllowDBNull();

            dtNodeInfo.Columns.Add("Diagnostic Data Quality", typeof(string))
                            .AllowDBNull();

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

            dtNodeInfo.Columns.Add("VNodes", typeof(bool)).AllowDBNull = true;
            dtNodeInfo.Columns.Add("Nbr VNodes", typeof(int)).AllowDBNull = true;
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

                Logger.Instance.Info("Loading Node Information");

                var logEvtsCollection = (from logEvt in this.Cluster.GetAllEventsReadOnly(DSEDiagnosticLibrary.LogCassandraEvent.ElementCreationTypes.DCNodeKSDDLTypeClassOnly)
                                         group logEvt by new { logEvt.DataCenter, logEvt.Node } into dcGrp
                                         let logDCEvts = dcGrp.Where(dcItem => dcItem.Class.HasFlag(EventClasses.Compaction)
                                                                                 || dcItem.Class.HasFlag(EventClasses.Flush)
                                                                                 || dcItem.Class.HasFlag(EventClasses.Repair)
                                                                                 || dcItem.Class.HasFlag(EventClasses.GC)).ToArray()
                                         let logKSEvts = (from dcItem in logDCEvts
                                                          where dcItem.Keyspace != null
                                                          group dcItem by new
                                                          {
                                                              IsSystem = dcItem.Keyspace.IsDSEKeyspace || dcItem.Keyspace.IsSystemKeyspace,
                                                              dcItem.Class
                                                          } into grpKS
                                                          select
                                                          new
                                                          {
                                                              grpKS.Key.IsSystem,
                                                              grpKS.Key.Class,
                                                              Count = grpKS.LongCount()
                                                          }).ToArray()
                                         select new
                                         {
                                             DC = dcGrp.Key.DataCenter,
                                             dcGrp.Key.Node,
                                             TotEvts = dcGrp.LongCount(),
                                             CompactionEvts = logKSEvts
                                                                .Where(i => i.Class.HasFlag(EventClasses.Compaction))
                                                                .Select(i => i.Count)
                                                                .DefaultIfEmpty().Sum(),
                                             FlushEvts = logKSEvts
                                                            .Where(i => i.Class.HasFlag(EventClasses.Flush))
                                                            .Select(i => i.Count)
                                                            .DefaultIfEmpty().Sum(),
                                             RepairEvts = logDCEvts.LongCount(e => e.Class.HasFlag(EventClasses.Repair)),
                                             GCEvts = logDCEvts.LongCount(e => e.Class.HasFlag(EventClasses.GC)),
                                             KSEvts = logKSEvts
                                         }).ToArray();

                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading Node Information for DC \"{0}\"", dataCenter.Name);

                    var statCollection = (from attrib in dataCenter.Nodes.SelectMany(d => ((DSEDiagnosticLibrary.Node)d).AggregatedStatsUnSafe)
                                          where attrib.Node != null
                                                 && attrib.Keyspace != null
                                                 && attrib.TableViewIndex != null
                                                 && attrib.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Node)
                                          group attrib by new
                                          {
                                              attrib.DataCenter,
                                              attrib.Node,
                                              attrib.Keyspace
                                          } into grpData
                                          let grpDataValues = grpData.SelectMany(d => ((DSEDiagnosticLibrary.AggregatedStats)d).DataUnSafe
                                                                                            .Where(a => (a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage
                                                                                                                || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount
                                                                                                                || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount
                                                                                                                || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount
                                                                                                                || DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(a.Key))
                                                                                                            && a.Value != null))
                                          let grpLWTValues = grpData.Where(i => i.TableViewIndex.FullName == Properties.Settings.Default.SystemLWTTableName)
                                                                   .SelectMany(d => ((DSEDiagnosticLibrary.AggregatedStats)d).DataUnSafe
                                                                                            .Where(a => (a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                                                                                            && a.Value != null))
                                          let grpBatchesValues = grpData.Where(i => i.TableViewIndex.FullName == Properties.Settings.Default.SystemBatchLogTableName
                                                                                        || i.TableViewIndex.FullName == Properties.Settings.Default.SystemBatchTableName)
                                                                            .SelectMany(d => ((DSEDiagnosticLibrary.AggregatedStats)d).DataUnSafe
                                                                                                        .Where(a => (a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                                                                                                        && a.Value != null))
                                          let grpNodeSyncValues = grpData.Where(i => i.TableViewIndex.FullName == Properties.Settings.Default.SystemNodeSuncStatusTableName
                                                                                  || i.TableViewIndex.FullName == Properties.Settings.Default.SystemNodeSuncValdateTableName)
                                                                            .SelectMany(d => ((DSEDiagnosticLibrary.AggregatedStats)d).DataUnSafe
                                                                                                  .Where(a => (a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount
                                                                                                                    || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount)
                                                                                                                  && a.Value != null))
                                          let grpNbrCompSpaceWarnings = grpData.SelectMany(d => ((DSEDiagnosticLibrary.AggregatedStats)d).DataUnSafe
                                                                            .Where(a => (a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.CompactionInsufficientSpace)
                                                                                            && a.Value != null))
                                          select new
                                          {
                                              DC = grpData.Key.DataCenter,
                                              grpData.Key.Node,
                                              grpData.Key.Keyspace,
                                              StorageTotal = grpDataValues.Where(a => a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage)
                                                                                            .Select(a => (DSEDiagnosticLibrary.UnitOfMeasure)a.Value)
                                                                                            .Where(a => !a.NaN)
                                                                                            .Select(a => a.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB))
                                                                                            .DefaultIfEmpty()
                                                                                            .Sum(),
                                              SSTablesTotal = grpDataValues.Where(a => a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount)
                                                                                            .Select(a => (long)(dynamic)a.Value)
                                                                                            .DefaultIfEmpty()
                                                                                            .Sum(),
                                              ReadTotal = grpDataValues.Where(a => a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount)
                                                                                            .Select(a => (decimal)(dynamic)a.Value)
                                                                                            .DefaultIfEmpty()
                                                                                            .Sum(),
                                              WriteTotal = grpDataValues.Where(a => a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                                                                            .Select(a => (decimal)(dynamic)a.Value)
                                                                                            .DefaultIfEmpty()
                                                                                            .Sum(),
                                              KeyTotal = grpDataValues.Where(a => DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(a.Key))
                                                                                            .Select(a => (decimal)(dynamic)a.Value)
                                                                                            .DefaultIfEmpty()
                                                                                            .Sum(),
                                              LWTTotal = grpLWTValues.Select(a => (long)(dynamic)a.Value)
                                                                                            .DefaultIfEmpty()
                                                                                            .Sum(),
                                              BatchesTotal = grpBatchesValues.Select(a => (long)(dynamic)a.Value)
                                                                                .DefaultIfEmpty()
                                                                                .Sum(),
                                              NodeSyncTotal = grpNodeSyncValues.Select(a => (long)(dynamic)a.Value)
                                                                                .DefaultIfEmpty()
                                                                                .Sum(),
                                              NbrCompStorageWarnings = grpNbrCompSpaceWarnings.Count()
                                          }).ToArray();

                    var dcTotStorage = dataCenter.Nodes
                                        .Where(n => !n.DSE.StorageUsed.NaN)
                                        .Select(n => n.DSE.StorageUsed.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB))
                                        .DefaultIfEmpty()
                                        .Sum();
                    var dcTotSSTables = dataCenter.Nodes.Select(n => n.DSE.SSTableCount).DefaultIfEmpty().Sum();
                    var dcTotReads = (ulong)dataCenter.Nodes.Select(n => n.DSE.ReadCount).DefaultIfEmpty().Sum();
                    var dcTotWrites = (ulong)dataCenter.Nodes.Select(n => n.DSE.WriteCount).DefaultIfEmpty().Sum();
                    var dcTotKeys = (ulong)dataCenter.Nodes.Select(n => n.DSE.KeyCount).DefaultIfEmpty().Sum();
                    long dcTotEvents = 0;
                    long dcTotFlushes = 0;
                    long dcTotCompactions = 0;
                    long dcTotRepairs = 0;
                    long dcTotGCs = 0;
                    var dclogEvtsCollection = logEvtsCollection.Where(i => i.DC == dataCenter);

                    if (dclogEvtsCollection.HasAtLeastOneElement())
                    {
                        dcTotEvents = dclogEvtsCollection.Sum(i => i.TotEvts);
                        dcTotFlushes = dclogEvtsCollection.Sum(i => i.FlushEvts);
                        dcTotCompactions = dclogEvtsCollection.Sum(i => i.CompactionEvts);
                        dcTotRepairs = dclogEvtsCollection.Sum(i => i.RepairEvts);
                        dcTotGCs = dclogEvtsCollection.Sum(i => i.GCEvts);
                    }

                    foreach (var node in dataCenter.Nodes)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);
                        dataRow.SetField(ColumnNames.NodeIPAddress, node.NodeName());
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

                        if(node.DSE.NbrTokens.HasValue) dataRow.SetField("Nbr VNodes", (int) node.DSE.NbrTokens.Value);
                        //if(node.DSE.NbrExceptions.HasValue) dataRow.SetField("Nbr of Exceptions", (int) node.DSE.NbrExceptions.Value);
                        //dataRow.SetFieldToDecimal("Percent Repaired", node.DSE.RepairedPercent);
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

                        {
                            var nodeStats = statCollection.Where(i => i.Node == node).ToArray();
                            var nodeLogEvtStats = dclogEvtsCollection.Where(i => i.Node == node).ToArray();

                            {
                                var nbrStarts = node.StateChanges.Count(s => (s.State & NodeStateChange.DetectedStates.Started) == NodeStateChange.DetectedStates.Started
                                                                            && s.State != NodeStateChange.DetectedStates.Restarted);
                                var nbrRestarts = node.StateChanges.Count(s => s.State == NodeStateChange.DetectedStates.Restarted);
                                var nbrLngPauses = node.StateChanges.Count(s => (s.State & NodeStateChange.DetectedStates.LongPause) == NodeStateChange.DetectedStates.LongPause);
                                var nbrNotResponding = node.StateChanges.Count(s => (s.State & NodeStateChange.DetectedStates.NotResponding) == NodeStateChange.DetectedStates.NotResponding);
                                var nbrDead = node.StateChanges.Count(s => (s.State & NodeStateChange.DetectedStates.Dead) == NodeStateChange.DetectedStates.Dead);
                                var nbrNetwork = node.StateChanges.Count(s => (s.State & NodeStateChange.DetectedStates.NetworkEvent) == NodeStateChange.DetectedStates.NetworkEvent);
                                var nbrOther = node.StateChanges
                                                .Where(s => NodeStateChange.DetectedStates.OtherStates.HasFlag(s.State))
                                                .DuplicatesRemoved(s => s.State)
                                                .Count();

                                if (nbrStarts > 0)
                                    dataRow.SetField("Status Starts", nbrStarts);

                                if (nbrRestarts > 0)
                                    dataRow.SetField("Status Restarts", nbrRestarts);

                                if (nbrLngPauses > 0)
                                    dataRow.SetField("Long Down Pause", nbrLngPauses);

                                if (nbrNotResponding > 0)
                                    dataRow.SetField("Status Not Responding", nbrNotResponding);

                                if (nbrDead > 0)
                                    dataRow.SetField("Status Dead", nbrDead);

                                if (nbrNetwork > 0)
                                    dataRow.SetField("Network Events", nbrNetwork);

                                if (nbrOther > 0)
                                    dataRow.SetField("Status Other", nbrOther);
                            }
                            {
                                var device = node.DSE.Devices.Data?.FirstOrDefault();
                                var percentUtilized = node.Machine.Devices.PercentUtilized;

                                if (device != null && percentUtilized != null)
                                {
                                    var devicePercent = percentUtilized
                                                        .Where(i => i.Key.EndsWith('/' + device) || i.Key.EndsWith('/' + device + '1'))
                                                        .FirstOrDefault().Value;
                                    dataRow.SetField("Device Utilized", devicePercent);
                                }
                            }
                            {
                                var dcInsufficientSpace = nodeStats.Sum(i => i.NbrCompStorageWarnings);

                                if (dcInsufficientSpace > 0)
                                    dataRow.SetField("DU Insufficient Space", dcInsufficientSpace);
                            }
                            {
                                if (dcTotStorage > 0)
                                {
                                    var nodeTotStorage = nodeStats.Select(i => i.StorageTotal).DefaultIfEmpty().Sum();

                                    dataRow.SetField("Storage Total", nodeTotStorage);
                                    dataRow.SetField("Storage Percent", nodeTotStorage / dcTotStorage);

                                    {
                                        var nodeUserTot = nodeStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                            .Select(i => i.StorageTotal).DefaultIfEmpty().Sum();

                                        if (nodeUserTot > 0)
                                            dataRow.SetField("Storage Total (User)", nodeUserTot);
                                    }
                                }

                                if (dcTotSSTables > 0)
                                {
                                    var nodeTotSSTables = nodeStats.Select(i => i.SSTablesTotal).DefaultIfEmpty().Sum();

                                    dataRow.SetField("SSTables Total", nodeTotSSTables);
                                    dataRow.SetField("SSTables Percent", (decimal)nodeTotSSTables / (decimal)dcTotSSTables);

                                    {
                                        var nodeUserTot = nodeStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                            .Select(i => i.SSTablesTotal).DefaultIfEmpty().Sum();

                                        if (nodeUserTot > 0)
                                            dataRow.SetField("SSTables Total (User)", nodeUserTot);
                                    }
                                }
                            }
                            {
                                if (dcTotReads > 0)
                                {
                                    var nodeTotReads = nodeStats.Select(i => i.ReadTotal).DefaultIfEmpty().Sum();

                                    dataRow.SetField("Reads Total", nodeTotReads);
                                    dataRow.SetField("Reads Percent", (decimal)nodeTotReads / (decimal)dcTotReads);

                                    {
                                        var nodeUserTot = nodeStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                            .Select(i => i.ReadTotal).DefaultIfEmpty().Sum();

                                        if (nodeUserTot > 0)
                                            dataRow.SetField("Reads Total (User)", nodeUserTot);
                                    }
                                }

                                if (dcTotWrites > 0)
                                {
                                    var nodeTotWrites = nodeStats.Select(i => i.WriteTotal).DefaultIfEmpty().Sum();

                                    dataRow.SetField("Writes Total", nodeTotWrites);
                                    dataRow.SetField("Writes Percent", (decimal)nodeTotWrites / (decimal)dcTotWrites);

                                    {
                                        var nodeUserTot = nodeStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                            .Select(i => i.WriteTotal).DefaultIfEmpty().Sum();

                                        if (nodeUserTot > 0)
                                            dataRow.SetField("Writes Total (User)", nodeUserTot);
                                    }

                                    {
                                        var nodeLWTTot = nodeStats.Sum(i => i.LWTTotal);
                                        var nodeBatchTot = nodeStats.Sum(i => i.BatchesTotal);

                                        if (nodeLWTTot > 0)
                                            dataRow.SetField("LWT Percent", (decimal)nodeLWTTot / (decimal)nodeTotWrites);

                                        if (nodeBatchTot > 0)
                                            dataRow.SetField("Batch Percent", (decimal)nodeBatchTot / (decimal)nodeTotWrites);
                                    }
                                }
                            }
                            {

                                long nodeLogEvts = 0;
                                long nodeGCs = 0;
                                long nodeFlushes = 0;
                                long nodeCompactions = 0;

                                if (nodeLogEvtStats.HasAtLeastOneElement())
                                {                                    
                                    var nodeKSEvts = nodeLogEvtStats.SelectMany(d => d.KSEvts);

                                    if (nodeKSEvts.HasAtLeastOneElement())
                                    {
                                        if (dcTotEvents > 0)
                                        {
                                            nodeLogEvts = nodeLogEvtStats
                                                            .Select(i => i.TotEvts)
                                                            .DefaultIfEmpty()
                                                            .Sum();

                                            dataRow.SetField("Log Event Total", nodeLogEvts);
                                            dataRow.SetField("Log Event Percent", (decimal)nodeLogEvts / (decimal)dcTotEvents);
                                        }

                                        if (dcTotGCs > 0)
                                        {
                                            nodeGCs = nodeLogEvtStats
                                                    .Select(i => i.GCEvts)
                                                    .DefaultIfEmpty()
                                                    .Sum();

                                            dataRow.SetField("GC Events Total", nodeGCs);
                                            dataRow.SetField("GC Events Percent", (decimal)nodeGCs / (decimal)dcTotGCs);
                                        }

                                        if (dcTotFlushes > 0)
                                        {
                                            var classStats = nodeKSEvts
                                                            .Where(i => i.Class.HasFlag(EventClasses.Flush));
                                            nodeFlushes = classStats
                                                        .Select(i => i.Count)
                                                        .DefaultIfEmpty()
                                                        .Sum();
                                            var classTotUser = classStats
                                                                .Where(d => !d.IsSystem)
                                                                .Select(i => i.Count)
                                                                .DefaultIfEmpty()
                                                                .Sum();

                                            dataRow.SetField("Flush Total", nodeFlushes);
                                            dataRow.SetField("Flush Percent", (decimal)nodeFlushes / (decimal)dcTotFlushes);

                                            if (classTotUser > 0)
                                                dataRow.SetField("Flush Total (User)", classTotUser);
                                        }
                                        if (dcTotCompactions > 0)
                                        {
                                            var classStats = nodeKSEvts
                                                                .Where(i => i.Class.HasFlag(EventClasses.Compaction));
                                            nodeCompactions = classStats
                                                                .Select(i => i.Count)
                                                                .DefaultIfEmpty()
                                                                .Sum();
                                            var classTotUser = classStats
                                                                .Where(d => !d.IsSystem)
                                                                .Select(i => i.Count)
                                                                .DefaultIfEmpty()
                                                                .Sum();

                                            dataRow.SetField("Compaction Total", nodeCompactions);
                                            dataRow.SetField("Compaction Percent", (decimal)nodeCompactions / (decimal)dcTotCompactions);

                                            if (classTotUser > 0)
                                                dataRow.SetField("Compaction Total (User)", classTotUser);
                                        }
                                        if (dcTotRepairs > 0)
                                        {
                                            var classTotAll = nodeLogEvtStats
                                                                    .Select(i => i.RepairEvts)
                                                                    .DefaultIfEmpty()
                                                                    .Sum();

                                            dataRow.SetField("Repair Total", classTotAll);
                                            dataRow.SetField("Repair Percent", (decimal)classTotAll / (decimal)dcTotRepairs);

                                            {
                                                var classTotUser = nodeKSEvts
                                                                    .Where(i => i.Class.HasFlag(EventClasses.Repair))
                                                                    .Select(i => i.Count)
                                                                    .DefaultIfEmpty()
                                                                    .Sum();
                                                if (classTotUser > 0)
                                                {
                                                    dataRow.SetField("Repair Total (User)", classTotUser);

                                                    var vNodes = (long)(node.DSE.NbrTokens.HasValue ? (int)node.DSE.NbrTokens.Value : 1);
                                                    var totTables = dataCenter.Keyspaces.Sum(k => (long)(k.Stats.MaterialViews + k.Stats.Tables));

                                                    dataRow.SetField("Repair Est Tasks (User)", (long)vNodes * totTables);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            {
                                var nodeNodeSyncTrans = nodeStats.Sum(i => i.NodeSyncTotal);

                                if (nodeNodeSyncTrans > 0)
                                    dataRow.SetField("Node Sync Count", nodeNodeSyncTrans);
                            }
                            {
                                var nodeQualityFactor = node.DataQualityFactor();
                                var inds = new string[] { "Poor", "OK", "Good", "Excellent" };
                                var offset = (decimal)((dataCenter.LogDebugFiles > 0 ? 0 : 1)
                                                            + (dataCenter.LogSystemFiles > 0 ? 0 : 2));
                                var audit = string.Empty;

                                if (dataCenter.LogSystemFiles == 0 || dataCenter.LogDebugFiles == 0)
                                    ++offset;
                                
                                var dqType = string.Empty;
                                decimal lastAdjNodeFactor = 0m;
                                var divFactor = Node.DataQualityMaxFactor / Properties.Settings.Default.DCDQDivideFactor;

                                for (int f = 4; f > 0; --f)
                                {
                                    var adjNodeFactor = ((decimal)f * divFactor) - offset;  //(f * (maxNodeDQ/4)) - offset

                                    if (Logger.Instance.IsDebugEnabled)
                                    {
                                        Logger.Instance.InfoFormat("Diagnostic Data Quality for Node \"{0}\" (DC {1}), Node Factor {2:###,###,##0.00}, Max. Factor: {3:###,###,##0.00}, Adjusted/Compare Factor: {4:###,###,##0.00}, Divide Factor: {5}, OffSet: {6}, Index: {7}",
                                                                    node.NodeName(),
                                                                    dataCenter.Name,
                                                                    nodeQualityFactor,
                                                                    Node.DataQualityMaxFactor,
                                                                    adjNodeFactor,
                                                                    divFactor,
                                                                    offset,
                                                                    f);
                                    }

                                    if (nodeQualityFactor >= adjNodeFactor)
                                    {
                                        var dqFactor = f - 1;
                                        
                                        if (lastAdjNodeFactor > 0m
                                                && dqFactor > 0
                                                && nodeQualityFactor >= ((lastAdjNodeFactor - adjNodeFactor) / 2m) + adjNodeFactor)
                                        {
                                            dataRow.SetField("Diagnostic Data Quality", dqType = inds[dqFactor] + "+");
                                        }
                                        else
                                        {
                                            dataRow.SetField("Diagnostic Data Quality", dqType = inds[dqFactor]);
                                        }
                                        break;
                                    }
                                    lastAdjNodeFactor = adjNodeFactor;
                                }

                                if (string.IsNullOrEmpty(dqType))
                                {
                                    dataRow.SetField("Diagnostic Data Quality", dqType = "Poor-");                                    
                                }

                                Logger.Instance.InfoFormat("Diagnostic Data Quality for Node \"{0}\" (DC {5}), Factor {1:###,###,##0.00} (\"{3}\") Max. Factor: {4:###,###,##0.00}, Offset {2}, Audit: {6}",
                                                            node.NodeName(),                                                            
                                                            nodeQualityFactor,                                                            
                                                            offset,
                                                            dqType,
                                                            Node.DataQualityMaxFactor,
                                                            dataCenter.Name,
                                                            audit);
                            }
                        }

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
