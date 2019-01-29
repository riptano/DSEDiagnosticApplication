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
    public sealed class DataCenterDataTable : DataTableLoad
    {
        public DataCenterDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {            
        }
        
        public override DataTable CreateInitializationTable()
        {
            var dtDCInfo = new DataTable(TableNames.DataCenters, TableNames.Namespace);
            dtDCInfo.CaseSensitive();

            if (this.SessionId.HasValue) dtDCInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtDCInfo.Columns.Add(ColumnNames.DataCenter, typeof(string)).EnableUnique();
            dtDCInfo.PrimaryKey = new System.Data.DataColumn[] { dtDCInfo.GetColumn(ColumnNames.DataCenter) };

            dtDCInfo.Columns.Add("Total Nodes", typeof(long));

            dtDCInfo.Columns.Add("Cassandra", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Search", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Analytics", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Graph", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Multi-Instance", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Other", typeof(long))
                            .AllowDBNull();

            dtDCInfo.Columns.Add("Status Up", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Status Down", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Status Unknown", typeof(long))
                            .AllowDBNull();

            dtDCInfo.Columns.Add("DSE Version", typeof(string))
                            .AllowDBNull();

            //Device Utilization 
            dtDCInfo.Columns.Add("DU Max", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("DU Max-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("DU Min", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("DU Min-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("DU Avg", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("DU Insufficient Space", typeof(long))
                            .AllowDBNull();

            //Storage
            dtDCInfo.Columns.Add("Storage Max", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Storage Max-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Storage Min", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Storage Min-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Storage Avg", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Storage Total", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Storage Total (User)", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Storage Percent", typeof(decimal))
                            .AllowDBNull();

            //SSTables
            dtDCInfo.Columns.Add("SSTables Max", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables Max-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables Min", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables Min-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables Avg", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables StdDev", typeof(double))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables Total", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables Total (User)", typeof(long))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("SSTables Percent", typeof(decimal))
                            .AllowDBNull();

            //Distribution
            dtDCInfo.Columns.Add("Distribution Storage From", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution Storage To", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution Storage StdDev", typeof(double))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution Keys From", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution Keys To", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution Keys StdDev", typeof(double))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution SSTables From", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution SSTables To", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Distribution SSTables StdDev", typeof(double))
                            .AllowDBNull();

            dtDCInfo.Columns.Add("Active Tbls/Idxs/Vws", typeof(decimal))
                           .AllowDBNull();

            //Counts
            dtDCInfo.Columns.Add("Reads Max", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Reads Max-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Reads Min", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Reads Min-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Reads Avg", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Reads StdDev", typeof(double))
                           .AllowDBNull();
            dtDCInfo.Columns.Add("Reads Total", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Reads Total (User)", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Reads Percent", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Max", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Max-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Min", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Min-Node", typeof(string))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Avg", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes StdDev", typeof(double))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Total", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Total (User)", typeof(ulong))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Writes Percent", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("Batch Percent", typeof(decimal))
                            .AllowDBNull();
            dtDCInfo.Columns.Add("LWT Percent", typeof(decimal))
                            .AllowDBNull();

            dtDCInfo.DefaultView.ApplyDefaultSort = false;
            dtDCInfo.DefaultView.AllowDelete = false;
            dtDCInfo.DefaultView.AllowEdit = false;
            dtDCInfo.DefaultView.AllowNew = false;
            dtDCInfo.DefaultView.Sort = string.Format("[{0}] ASC", ColumnNames.DataCenter);

            return dtDCInfo;
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

                Logger.Instance.InfoFormat("Loading DataCenter Information");
                this.CancellationToken.ThrowIfCancellationRequested();

                var statCollection = (from attrib in this.Cluster.Nodes.SelectMany(d => ((DSEDiagnosticLibrary.Node)d).AggregatedStatsUnSafe)
                                     where attrib.DataCenter != null
                                            && attrib.Node != null
                                            && attrib.Keyspace != null
                                            && attrib.TableViewIndex != null
                                            && attrib.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Node)
                                     group attrib by new { attrib.DataCenter,
                                                            attrib.Node,
                                                            attrib.Keyspace } into grpData
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
                                       let grpNbrCompSpaceWarnings = grpData.SelectMany(d => ((DSEDiagnosticLibrary.AggregatedStats)d).DataUnSafe
                                                                        .Where(a => (a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.CompactionInsufficientSpace)
                                                                                        && a.Value != null))
                                      select new {
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
                                                    NbrCompStorageWarnings = grpNbrCompSpaceWarnings.Count()
                                      }).ToArray();

                var clusterTotStorage = this.Cluster.Nodes
                                        .Where(n => !n.DSE.StorageUsed.NaN)
                                        .DefaultIfEmpty()
                                        .Sum(n => n.DSE.StorageUsed.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB));
                var clusterTotSSTables = this.Cluster.Nodes.DefaultIfEmpty().Sum(n => n.DSE.SSTableCount);
                var clusterTotReads = (ulong)this.Cluster.Nodes.DefaultIfEmpty().Sum(n => n.DSE.ReadCount);
                var clusterTotWrites = (ulong)this.Cluster.Nodes.DefaultIfEmpty().Sum(n => n.DSE.WriteCount);
                var clusterTotKeys = (ulong)this.Cluster.Nodes.DefaultIfEmpty().Sum(n => n.DSE.KeyCount);

                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading DataCenter Information for DC \"{0}\"", dataCenter.Name);

                    dataRow = this.Table.NewRow();

                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);

                    var dcNodes = dataCenter.Nodes.ToArray();
                    var totNodes = dcNodes.Count();

                    dataRow.SetField("Total Nodes", totNodes);

                    if (totNodes > 0)
                    {                        
                        {
                            var nbrCassandra = dcNodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Cassandra));
                            var nbrSearch = dcNodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Search));
                            var nbrAnalytics = dcNodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Analytics));
                            var nbrGraph = dcNodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Graph));
                            var nbrMI = dcNodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.MultiInstance));

                            if (nbrCassandra > 0)
                                dataRow.SetField("Cassandra", nbrCassandra);
                            if (nbrSearch > 0)
                                dataRow.SetField("Search", nbrSearch);
                            if (nbrAnalytics > 0)
                                dataRow.SetField("Analytics", nbrAnalytics);
                            if (nbrGraph > 0)
                                dataRow.SetField("Graph", nbrGraph);
                            if (nbrMI > 0)
                                dataRow.SetField("Multi-Instance", nbrMI);

                            var nbrOthers = totNodes - (nbrCassandra + nbrGraph + nbrAnalytics + nbrSearch);

                            if (nbrOthers > 0)
                                dataRow.SetField("Other", nbrOthers);
                        }

                        {
                            var nbrUp = dcNodes.Count(i => i.DSE.Statuses == DSEDiagnosticLibrary.DSEInfo.DSEStatuses.Up);
                            var nbrDown = dcNodes.Count(i => i.DSE.Statuses == DSEDiagnosticLibrary.DSEInfo.DSEStatuses.Down);

                            dataRow.SetField("Status Up", nbrUp);
                            dataRow.SetField("Status Down", nbrDown);
                            dataRow.SetField("Status Unknown", totNodes - (nbrUp + nbrDown));
                        }

                        {
                            var devices = (from node in dcNodes
                                          let device = node.DSE.Devices.Data?.FirstOrDefault()
                                          let percentUtilized = node.Machine.Devices.PercentUtilized
                                          where device != null
                                                && percentUtilized != null
                                          select new
                                          {
                                              Node = node,
                                              DevicePercent = percentUtilized
                                                                .Where(i => i.Key.EndsWith('/' + device) || i.Key.EndsWith('/' + device + '1'))
                                                                .FirstOrDefault().Value
                                          }).ToArray();

                            if (devices.HasAtLeastOneElement())
                            {
                                var maxDevice = devices.Max((instance) => instance.DevicePercent, (ignore, instance) => instance);
                                var minDevice = devices.Min((instance) => instance.DevicePercent, (ignore, instance) => instance);
                                var avgDevice = devices.Average(i => i.DevicePercent);

                                dataRow.SetField("DU Max", maxDevice.DevicePercent);
                                dataRow.SetField("DU Max-Node", maxDevice.Node.Id.NodeName());
                                dataRow.SetField("DU Min", minDevice.DevicePercent);
                                dataRow.SetField("DU Min-Node", minDevice.Node.Id.NodeName());
                                dataRow.SetField("DU Avg", avgDevice);
                            }                           
                        }

                        {
                            var dseVersion = dcNodes.GroupBy(n => n.DSE.Versions.DSE).Select(g => g.Key).Where(g => g != null);
                            var dseSchema = dcNodes.GroupBy(n => n.DSE.Versions.Schema).Select(g => g.Key).Where(g => g.HasValue);

                            if (dseVersion.Count() > 1) dataRow.SetField("DSE Version", "MisMatched");
                            else if (dseVersion.HasAtLeastOneElement())
                            {
                                if(dseSchema.Count() > 1)
                                    dataRow.SetField("DSE Version", "MisMatched");
                                else
                                    dataRow.SetField("DSE Version", dseVersion.First().ToString());
                            }
                        }

                        {
                            var totalsNodes = dataCenter.Nodes
                                                    .Select(n => new
                                                    {
                                                        Node = n,
                                                        StorageTotal = n.DSE.StorageUsed.NaN ? 0 : n.DSE.StorageUsed.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB),
                                                        SSTablesTotal = n.DSE.SSTableCount,
                                                        ReadTotal = n.DSE.ReadCount,
                                                        WriteTotal = n.DSE.WriteCount,
                                                        KeyTotal = n.DSE.KeyCount
                                                    }).ToArray();

                            var dcTotStorage = totalsNodes.Sum(n => n.StorageTotal);
                            var dcTotSSTables = totalsNodes.Sum(n => n.SSTablesTotal);
                            var dcTotReads = (ulong)totalsNodes.Sum(n => n.ReadTotal);
                            var dcTotWrites = (ulong)totalsNodes.Sum(n => n.WriteTotal);
                            var dcTotKeys = (ulong)totalsNodes.Sum(n => n.KeyTotal);
                            var dcStats = statCollection.Where(i => i.DC.Equals(dataCenter)).ToArray();

                            if (dcTotStorage > 0)
                            {
                                var maxNode = totalsNodes.Max((instance) => instance.StorageTotal, (ignore, instance) => instance);
                                var minNode = totalsNodes.Min((instance) => instance.StorageTotal, (ignore, instance) => instance);
                                var avgValue = totalsNodes.Average(i => i.StorageTotal);

                                dataRow.SetField("Storage Max", maxNode.StorageTotal);
                                dataRow.SetField("Storage Max-Node", maxNode.Node.Id.NodeName());
                                dataRow.SetField("Storage Min", minNode.StorageTotal);
                                dataRow.SetField("Storage Min-Node", minNode.Node.Id.NodeName());
                                dataRow.SetField("Storage Avg", avgValue);
                                dataRow.SetField("Storage Total", dcTotStorage);
                                dataRow.SetField("Storage Percent", dcTotStorage / clusterTotStorage);

                                {
                                    var dcUserTot = dcStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                        .Select(i => i.StorageTotal).DefaultIfEmpty().Sum();

                                    if (dcUserTot > 0)
                                        dataRow.SetField("Storage Total (User)", dcUserTot);
                                }
                            }

                            if (dcTotSSTables > 0)
                            {
                                var maxNode = totalsNodes.Max((instance) => instance.SSTablesTotal, (ignore, instance) => instance);
                                var minNode = totalsNodes.Min((instance) => instance.SSTablesTotal, (ignore, instance) => instance);
                                var avgValue = totalsNodes.Average(i => i.SSTablesTotal);
                                var stddevValue = totalsNodes.Select(i => i.SSTablesTotal).StandardDeviation();

                                dataRow.SetField("SSTables Max", maxNode.SSTablesTotal);
                                dataRow.SetField("SSTables Max-Node", maxNode.Node.Id.NodeName());
                                dataRow.SetField("SSTables Min", minNode.SSTablesTotal);
                                dataRow.SetField("SSTables Min-Node", minNode.Node.Id.NodeName());
                                dataRow.SetField("SSTables Avg", avgValue);
                                dataRow.SetField("SSTables StdDev", stddevValue);
                                dataRow.SetField("SSTables Total", dcTotSSTables);
                                dataRow.SetField("SSTables Percent", (decimal)dcTotSSTables / (decimal)clusterTotSSTables);

                                {
                                    var dcUserTot = dcStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                        .Select(i => i.SSTablesTotal).DefaultIfEmpty().Sum();

                                    if (dcUserTot > 0)
                                        dataRow.SetField("SSTables Total (User)", dcUserTot);
                                }
                            }

                            if (dcTotStorage > 0)
                            {
                                var storeageValues = totalsNodes.Select(d => d.StorageTotal / dcTotStorage);
                                var maxNode = storeageValues.Max();
                                var minNode = storeageValues.Min();
                                var stdDevNode = storeageValues.StandardDeviation();

                                dataRow.SetField("Distribution Storage From", maxNode);
                                dataRow.SetField("Distribution Storage To", minNode);
                                dataRow.SetField("Distribution Storage StdDev", stdDevNode);
                            }

                            if (dcTotSSTables > 0)
                            {
                                var storeageValues = totalsNodes.Select(d => (decimal)d.SSTablesTotal / (decimal)dcTotSSTables);
                                var maxNode = storeageValues.Max();
                                var minNode = storeageValues.Min();
                                var stdDevNode = storeageValues.StandardDeviation();

                                dataRow.SetField("Distribution SSTables From", maxNode);
                                dataRow.SetField("Distribution SSTables To", minNode);
                                dataRow.SetField("Distribution SSTables StdDev", stdDevNode);
                            }
                            if (dcTotKeys > 0)
                            {
                                var storeageValues = totalsNodes.Select(d => (decimal)d.KeyTotal / (decimal)dcTotKeys);
                                var maxNode = storeageValues.Max();
                                var minNode = storeageValues.Min();
                                var stdDevNode = storeageValues.StandardDeviation();

                                dataRow.SetField("Distribution Keys From", maxNode);
                                dataRow.SetField("Distribution Keys To", minNode);
                                dataRow.SetField("Distribution Keys StdDev", stdDevNode);
                            }

                            if (dcTotReads > 0)
                            {
                                var maxNode = totalsNodes.Max((instance) => instance.ReadTotal, (ignore, instance) => instance);
                                var minNode = totalsNodes.Min((instance) => instance.ReadTotal, (ignore, instance) => instance);
                                var avgValue = totalsNodes.Average(i => i.ReadTotal);
                                var stdDevNode = totalsNodes.Select(i => i.ReadTotal).StandardDeviationP();

                                dataRow.SetField("Reads Max", (ulong)maxNode.ReadTotal);
                                dataRow.SetField("Reads Max-Node", maxNode.Node.Id.NodeName());
                                dataRow.SetField("Reads Min", (ulong)minNode.ReadTotal);
                                dataRow.SetField("Reads Min-Node", minNode.Node.Id.NodeName());
                                dataRow.SetField("Reads Avg", avgValue);
                                dataRow.SetField("Reads StdDev", avgValue);
                                dataRow.SetField("Reads Total", dcTotReads);
                                dataRow.SetField("Reads Percent", (decimal)dcTotReads / (decimal)clusterTotReads);

                                {
                                    var dcUserTot = dcStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                        .Select(i => i.ReadTotal).DefaultIfEmpty().Sum();

                                    if (dcUserTot > 0)
                                        dataRow.SetField("Reads Total (User)", dcUserTot);
                                }
                            }

                            if (dcTotWrites > 0)
                            {
                                var maxNode = totalsNodes.Max((instance) => instance.WriteTotal, (ignore, instance) => instance);
                                var minNode = totalsNodes.Min((instance) => instance.WriteTotal, (ignore, instance) => instance);
                                var avgValue = totalsNodes.Average(i => i.WriteTotal);
                                var stdDevNode = totalsNodes.Select(i => i.WriteTotal).StandardDeviationP();

                                dataRow.SetField("Writes Max", (ulong)maxNode.ReadTotal);
                                dataRow.SetField("Writes Max-Node", maxNode.Node.Id.NodeName());
                                dataRow.SetField("Writes Min", (ulong)minNode.ReadTotal);
                                dataRow.SetField("Writes Min-Node", minNode.Node.Id.NodeName());
                                dataRow.SetField("Writes Avg", avgValue);
                                dataRow.SetField("Writes StdDev", stdDevNode);
                                dataRow.SetField("Writes Total", dcTotWrites);
                                dataRow.SetField("Writes Percent", (decimal)dcTotWrites / (decimal)clusterTotWrites);

                                {
                                    var dcUserTot = dcStats.Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace)
                                                        .Select(i => i.WriteTotal).DefaultIfEmpty().Sum();

                                    if (dcUserTot > 0)
                                        dataRow.SetField("Writes Total (User)", dcUserTot);
                                }

                            }

                            {
                                var dcInsufficientSpace = dcStats.Sum(i => i.NbrCompStorageWarnings);

                                if (dcInsufficientSpace > 0)
                                    dataRow.SetField("DU Insufficient Space", dcInsufficientSpace);
                            }                                                       

                            {
                                var dcLWTTot = dcStats.Sum(i => i.LWTTotal);
                                var dcBatchTot = dcStats.Sum(i => i.BatchesTotal);

                                if (dcLWTTot > 0)
                                    dataRow.SetField("LWT Percent", (decimal) dcLWTTot / (decimal) dcTotWrites);

                                if (dcBatchTot > 0)
                                    dataRow.SetField("Batch Percent", (decimal)dcBatchTot / (decimal)dcTotWrites);
                            }
                        }

                        {
                            var ksInDCTot = dataCenter.Keyspaces
                                                .Where(ks => !ks.IsDSEKeyspace && !ks.IsSystemKeyspace)
                                                .DefaultIfEmpty()
                                                .Sum(ks => (decimal) (ks.Stats.Tables
                                                                        + ks.Stats.MaterialViews
                                                                        + ks.Stats.SecondaryIndexes
                                                                        + ks.Stats.CustomIndexes
                                                                        + ks.Stats.SasIIIndexes));

                            if (ksInDCTot > 0m)
                            {
                                var ksInDCActive = dataCenter.Keyspaces.Where(ks => !ks.IsDSEKeyspace && !ks.IsSystemKeyspace).DefaultIfEmpty().Sum(ks => (decimal) ks.Stats.NbrActive);

                                dataRow.SetField("Active Tbls/Idxs/Vws", ksInDCActive/ksInDCTot);
                            }
                        }
                    }

                    this.Table.Rows.Add(dataRow);
                    ++nbrItems;                    

                    Logger.Instance.InfoFormat("Loaded DataCenter Information for DC \"{0}\", Total Nbr Items {1:###,###,##0}", dataCenter.Name, nbrItems);
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading DataCenter Information Canceled");
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
