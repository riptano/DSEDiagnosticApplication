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
    public sealed class DataCenterDataTable : DataTableLoad
    {
        public DataCenterDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtDCInfo = new DataTable(TableNames.DataCenters, TableNames.Namespace);

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

                var statCollection = from attrib in this.Cluster.Nodes.SelectMany(d => ((DSEDiagnosticLibrary.Node)d).AggregatedStatsUnSafe)
                                     where attrib.DataCenter != null
                                            && attrib.Node != null
                                            && attrib.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.KeyspaceTableViewIndexStats | DSEDiagnosticLibrary.EventClasses.Node)
                                     group attrib by new { attrib.DataCenter,
                                                            attrib.Node,
                                                            attrib.Keyspace } into grpData
                                     let grpDataValues = grpData.SelectMany(d => ((DSEDiagnosticLibrary.AggregatedStats)d).DataUnSafe
                                                                                        .Where(a => (a.Key == Properties.Settings.Default.CFStatsSpaceUsed
                                                                                                            || a.Key == Properties.Settings.Default.CFStatsSSTableCount
                                                                                                            || a.Key == Properties.Settings.Default.CFStatsLocalReadCountName 
                                                                                                            || a.Key == Properties.Settings.Default.CFStatsLocalWriteCountName 
                                                                                                            || a.Key == Properties.Settings.Default.CFStatsNbrKeys)
                                                                                                        && a.Value != null))
                                     select new {
                                                    DC = grpData.Key.DataCenter,
                                                    Node = grpData.Key.Node,
                                                    Keyspace = grpData.Key.Keyspace,
                                                    StorageTotal = grpDataValues.Where(a => a.Key == Properties.Settings.Default.CFStatsSpaceUsed)
                                                                            .Select(a => (DSEDiagnosticLibrary.UnitOfMeasure) a.Value)                                                                                                
                                                                            .Where(a => !a.NaN)
                                                                            .Select(a => a.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB))
                                                                            .DefaultIfEmpty()
                                                                            .Sum(),
                                                    SSTablesTotal = grpDataValues.Where(a => a.Key == Properties.Settings.Default.CFStatsSSTableCount)
                                                                            .Select(a => (long)(dynamic)a.Value)
                                                                            .DefaultIfEmpty()
                                                                            .Sum(),
                                                    ReadTotal = grpDataValues.Where(a => a.Key == Properties.Settings.Default.CFStatsLocalReadCountName)
                                                                            .Select(a => (decimal)(dynamic)a.Value)
                                                                            .DefaultIfEmpty()
                                                                            .Sum(),
                                                    WriteTotal = grpDataValues.Where(a => a.Key == Properties.Settings.Default.CFStatsLocalWriteCountName)
                                                                            .Select(a => (decimal)(dynamic)a.Value)
                                                                            .DefaultIfEmpty()
                                                                            .Sum(),
                                                    KeyTotal = grpDataValues.Where(a => a.Key == Properties.Settings.Default.CFStatsNbrKeys)
                                                                            .Select(a => (decimal)(dynamic)a.Value)
                                                                            .DefaultIfEmpty()
                                                                            .Sum()
                                                };

                var clusterTotStorage = statCollection.Sum(i => i.StorageTotal);
                var clusterTotSSTables = statCollection.Sum(i => i.SSTablesTotal);
                var clusterTotReads = (ulong) statCollection.Sum(i => i.ReadTotal);
                var clusterTotWrites = (ulong) statCollection.Sum(i => i.WriteTotal);
                var clusterTotKeys = (ulong) statCollection.Sum(i => i.KeyTotal);

                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading DataCenter Information for DC \"{0}\"", dataCenter.Name);

                    dataRow = this.Table.NewRow();

                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);

                    var totNodes = dataCenter.Nodes.Count();

                    dataRow.SetField("Total Nodes", totNodes);

                    if (totNodes > 0)
                    {                        
                        {
                            var nbrCassandra = dataCenter.Nodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Cassandra));
                            var nbrSearch = dataCenter.Nodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Search));
                            var nbrAnalytics = dataCenter.Nodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Analytics));
                            var nbrGraph = dataCenter.Nodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.Graph));
                            var nbrMI = dataCenter.Nodes.Count(i => i.DSE.InstanceType.HasFlag(DSEDiagnosticLibrary.DSEInfo.InstanceTypes.MultiInstance));

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
                            var nbrUp = dataCenter.Nodes.Count(i => i.DSE.Statuses == DSEDiagnosticLibrary.DSEInfo.DSEStatuses.Up);
                            var nbrDown = dataCenter.Nodes.Count(i => i.DSE.Statuses == DSEDiagnosticLibrary.DSEInfo.DSEStatuses.Down);

                            dataRow.SetField("Status Up", nbrUp);
                            dataRow.SetField("Status Down", nbrDown);
                            dataRow.SetField("Status Unknown", totNodes - (nbrUp + nbrDown));
                        }

                        {
                            var devices = from node in dataCenter.Nodes
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
                                          };

                            if (devices.HasAtLeastOneElement())
                            {
                                var maxValue = devices.Max(d => d.DevicePercent);
                                var minValue = devices.Where(d => d.DevicePercent > 0).DefaultIfEmpty().Min(d => d.DevicePercent);

                                var maxDevice = devices.FirstOrDefault(i => i.DevicePercent == maxValue);
                                var minDevice = devices.FirstOrDefault(i => i.DevicePercent == minValue);
                                var avgDevice = devices.Average(i => i.DevicePercent);

                                dataRow.SetField("DU Max", maxDevice.DevicePercent);
                                dataRow.SetField("DU Max-Node", maxDevice.Node.Id.NodeName());
                                dataRow.SetField("DU Min", minDevice.DevicePercent);
                                dataRow.SetField("DU Min-Node", minDevice.Node.Id.NodeName());
                                dataRow.SetField("DU Avg", avgDevice);
                            }
                        }

                        var dcStats = statCollection.Where(i => i.DC.Equals(dataCenter));

                        if (dcStats.HasAtLeastOneElement())
                        {
                            var dcTotStorage = dcStats.Sum(i => i.StorageTotal);
                            var dcTotSSTables = dcStats.Sum(i => i.SSTablesTotal);
                            var dcTotReads = (ulong)dcStats.Sum(i => i.ReadTotal);
                            var dcTotWrites = (ulong)dcStats.Sum(i => i.WriteTotal);
                            var dcTotKeys = (ulong)dcStats.Sum(i => i.KeyTotal);
                            var totalsNodes = dcStats.GroupBy(i => i.Node)
                                                    .Select(i => new { Node = i.Key,
                                                                        StorageTotal = i.DefaultIfEmpty().Sum(d => d.StorageTotal),
                                                                        SSTablesTotal = i.DefaultIfEmpty().Sum(d => d.SSTablesTotal),
                                                                        ReadTotal = i.DefaultIfEmpty().Sum(d => d.ReadTotal),
                                                                        WriteTotal = i.DefaultIfEmpty().Sum(d => d.WriteTotal),
                                                                        KeyTotal = i.DefaultIfEmpty().Sum(d => d.KeyTotal) });

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

                                if(this.IgnoreKeySpaces.Length > 0)
                                {
                                    var dcUserTot = dcStats.Where(i => !this.IgnoreKeySpaces.Any(k => k == i.Keyspace.Name)).DefaultIfEmpty().Sum(i => i.StorageTotal);

                                    if(dcUserTot > 0)
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
                                dataRow.SetField("SSTables Percent", (decimal) dcTotSSTables / (decimal) clusterTotSSTables);

                                if (this.IgnoreKeySpaces.Length > 0)
                                {
                                    var dcUserTot = dcStats.Where(i => !this.IgnoreKeySpaces.Any(k => k == i.Keyspace.Name)).DefaultIfEmpty().Sum(i => i.SSTablesTotal);

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
                                dataRow.SetField("Reads Percent", (decimal) dcTotReads / (decimal) clusterTotReads);

                                if (this.IgnoreKeySpaces.Length > 0)
                                {
                                    var dcUserTot = dcStats.Where(i => !this.IgnoreKeySpaces.Any(k => k == i.Keyspace.Name)).DefaultIfEmpty().Sum(i => i.ReadTotal);

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
                                dataRow.SetField("Writes Percent", (decimal) dcTotWrites / (decimal) clusterTotWrites);

                                if (this.IgnoreKeySpaces.Length > 0)
                                {
                                    var dcUserTot = dcStats.Where(i => !this.IgnoreKeySpaces.Any(k => k == i.Keyspace.Name)).DefaultIfEmpty().Sum(i => i.WriteTotal);

                                    if (dcUserTot > 0)
                                        dataRow.SetField("Writes Total (User)", dcUserTot);
                                }
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
