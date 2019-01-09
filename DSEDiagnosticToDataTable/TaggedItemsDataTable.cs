using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Common;
using DSEDiagnosticLogger;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticToDataTable
{
    public sealed class TaggedItemsDataTable : DataTableLoad
    {

        public TaggedItemsDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];            
        }

        public string[] IgnoreKeySpaces { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtStats = new DataTable(TableNames.TaggedItems, TableNames.Namespace);

            if (this.SessionId.HasValue) dtStats.Columns.Add(ColumnNames.SessionId, typeof(Guid));
            
           
            dtStats.Columns.Add(ColumnNames.KeySpace, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtStats.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtStats.Columns.Add("CQL Type", typeof(string)).AllowDBNull = true;

            dtStats.Columns.Add("Read Max", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Read Min", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Read Avg", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Read StdDev", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Read Factor", typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add("Read Percent", typeof(decimal)).AllowDBNull = true;
           
            dtStats.Columns.Add("Write Max", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Write Min", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Write Avg", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Write StdDev", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Write Factor", typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add("Write Percent", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Keys Percent", typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add("SSTables Max", typeof(long)).AllowDBNull = true;
            dtStats.Columns.Add("SSTables Min", typeof(long)).AllowDBNull = true;
            dtStats.Columns.Add("SSTables Avg", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("SSTables StdDev", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("SSTables Factor", typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add("SSTable Percent", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Storage Percent", typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add("Tombstone Ratio Max", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Tombstone Ratio Min", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Tombstone Ratio Avg", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Tombstone Ratio StdDev", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Tombstone Ratio Factor", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Tombstones Read", typeof(long)).AllowDBNull = true;
            dtStats.Columns.Add("Live Read", typeof(long)).AllowDBNull = true;

            dtStats.Columns.Add("Partition Size Max", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Partition Size Min", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Partition Size Avg", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Partition Size StdDev", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Partition Size Factor", typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add("Flush Size Max", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Flush Size Min", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Flush Size Avg", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Flush Size StdDev", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Flush Size Factor", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Flush Pending", typeof(long)).AllowDBNull = true;
            
            dtStats.Columns.Add("Weighted Factor Max", typeof(decimal));
            dtStats.Columns.Add("Weighted Factor Avg", typeof(decimal));

            dtStats.Columns.Add("Secondary Indexes", typeof(int)).AllowDBNull = true;
            dtStats.Columns.Add("Materialized Views", typeof(int)).AllowDBNull = true;
            dtStats.Columns.Add("Common Key", typeof(string)).AllowDBNull = true;

            dtStats.Columns.Add("Base Table", typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add("Base Table Weighted Factor Max", typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add("Base Table Weighted Factor Avg", typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(ColumnNames.ReconciliationRef, typeof(object)).AllowDBNull = true;

            dtStats.DefaultView.ApplyDefaultSort = false;
            dtStats.DefaultView.AllowDelete = false;
            dtStats.DefaultView.AllowEdit = false;
            dtStats.DefaultView.AllowNew = false;
            dtStats.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [{2}] ASC, [{3}] ASC",
                                                        ColumnNames.KeySpace,
                                                        ColumnNames.Table,
                                                        ColumnNames.DataCenter,
                                                        ColumnNames.NodeIPAddress
                                                        );

            return dtStats;
        }

        static readonly IList<decimal> EmptyDecimalList = new List<decimal>(0);

        public override DataTable LoadTable()
        {
            this.Table.BeginLoadData();
            Logger.Instance.Info("Start Loading Tagged Stats");

            try
            {
                this.CancellationToken.ThrowIfCancellationRequested();
               
                int nbrItems = 0;

                #region In-Line Functions
                IList<decimal> FlattenEnumerable(System.Collections.IEnumerable objValues)
                {
                    var decimalLst = new List<decimal>();

                    foreach(var item in objValues)
                    {
                        if(item == null) continue;

                        if(item is System.Collections.IEnumerable)
                        {
                            if (item is string) continue;
                            decimalLst.AddRange(FlattenEnumerable((System.Collections.IEnumerable)item));
                        }
                        else
                        {
                            decimalLst.AddRange(Flatten(item));
                        }
                    }

                    return decimalLst;
                }

                IList<decimal> Flatten(object objValues)
                {                    
                    if (objValues is System.Collections.IEnumerable)
                    {
                        if (objValues is string) return EmptyDecimalList;

                        return FlattenEnumerable((System.Collections.IEnumerable)objValues);
                    }
                    else if (objValues is UnitOfMeasure uom)
                    {                        
                        if (!uom.NaN)
                        {
                            if ((uom.UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.SizeUnits) != 0)
                            {
                                return new List<decimal>(1) { uom.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB) };
                            }
                            else if ((uom.UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.TimeUnits) != 0)
                            {
                                return new List<decimal>(1) { uom.ConvertTimeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MS) };
                            }
                            else
                            {
                                return new List<decimal>(1) { uom.Value };
                            }
                        }
                    }
                    else
                    {
                        return new List<decimal>(1) { (decimal)((dynamic)objValues) };
                    }
                    

                    return EmptyDecimalList;
                }

                #endregion

                var allStatCollection = this.Cluster.Nodes
                                        .SelectMany(d =>
                                                        ((DSEDiagnosticLibrary.Node)d).AggregatedStatsUnSafe
                                                            .Where(s => s.TableViewIndex != null && s.Data.Count > 0)
                                                           .SelectMany(s =>
                                                                            s.Data
                                                                                .Where(a => (a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrKeys
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrKeys1
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadLatency
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteLatency
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxTombstonesSlice
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstonesRead
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCell
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxLiveCellsReadSlice
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCellRatio
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.CompactedPartitionMax
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.PartitionLarge
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.RowLarge)
                                                                                                                        && a.Value != null)                                                                                        
                                                                                .Select(kv => new
                                                                                    {
                                                                                        s.DataCenter,
                                                                                        s.Node,
                                                                                        s.Keyspace,
                                                                                        s.TableViewIndex,
                                                                                        Property = kv.Key,
                                                                                        Values = Flatten(kv.Value).ToArray()
                                                                                    })
                                                                                 .Where(i => i.Values.Length > 0))
                                            )
                                            .ToArray();
                
                this.CancellationToken.ThrowIfCancellationRequested();

                var totStorage = allStatCollection
                                    .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage)
                                    .DefaultIfEmpty()
                                    .Sum(a => a.Values
                                                .Sum());
                var totKeys = allStatCollection
                                .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrKeys
                                                || i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrKeys1)
                                .DefaultIfEmpty()
                                .Sum(a => a.Values
                                                .Sum());
                var totReadCnt = allStatCollection
                                    .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount)
                                    .DefaultIfEmpty()
                                    .Sum(a => a.Values
                                                .Sum());
                var totWriteCnt = allStatCollection
                                    .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                    .DefaultIfEmpty()
                                    .Sum(a => a.Values
                                                .Sum());
                var totSSTables = allStatCollection
                                    .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount)
                                    .DefaultIfEmpty()
                                    .Sum(a => a.Values
                                                .Sum());

                Logger.Instance.InfoFormat("Loading {0} Possible Tagged Stats", allStatCollection.Length);

                var attrReadLatencyThreshold = DSEDiagnosticAnalytics.AttributeThreshold.FindAttrThreshold(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadLatency);
                var attrWriteLatencyThreshold = DSEDiagnosticAnalytics.AttributeThreshold.FindAttrThreshold(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteLatency);
                var attrSSTableThreshold = DSEDiagnosticAnalytics.AttributeThreshold.FindAttrThreshold(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount);
                var attrPartitionSizeThreshold = DSEDiagnosticAnalytics.AttributeThreshold.FindAttrThreshold(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.PartitionLarge);
                var attrTombstoneRatioThreshold = DSEDiagnosticAnalytics.AttributeThreshold.FindAttrThreshold(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCellRatio);

                //In-line Function
                Tuple<bool,decimal,decimal,decimal,decimal,decimal> MaxMinAvg(IEnumerable<decimal> values, DSEDiagnosticAnalytics.AttributeThreshold attrThreshold)
                {
                    if(values.IsEmpty())
                    {                        
                        return new Tuple<bool, decimal, decimal, decimal, decimal, decimal>(false, -1, -1, -1, -1, -1);
                    }

                    if(values.Count() == 1)
                    {
                        var checkValue = values.First();
                        var threshold1 = attrThreshold.DetermineWeightFactor(checkValue, out decimal weightFactor1);

                        return new Tuple<bool, decimal, decimal, decimal, decimal, decimal>(threshold1, weightFactor1, checkValue, -1, -1, 0);
                    }

                    var itemMax = values.Max();
                    var itemMin = values.Min();
                    var itemAvg = values.Average();
                    var itemStd = (decimal) values.StandardDeviationP();
                    var threshold = attrThreshold.DetermineWeightFactor(itemMax, itemMin, itemAvg, out decimal weightFactor);

                    return new Tuple<bool, decimal, decimal, decimal, decimal, decimal>(threshold, weightFactor, itemMax, itemMin, itemAvg, itemStd);
                }

                this.CancellationToken.ThrowIfCancellationRequested();

                var allNodeTableStatCollection = allStatCollection
                                                    .Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace && !i.Keyspace.IsPerformanceKeyspace)
                                                    .GroupBy(i => new { i.Keyspace, i.TableViewIndex, i.DataCenter, i.Node})
                                                    .Select(g =>
                                                    {
                                                        var readLatencyInfo = MaxMinAvg(g.Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadLatency)
                                                                                        .SelectMany(a => a.Values),
                                                                                     attrReadLatencyThreshold);
                                                        var writeLatencyInfo = MaxMinAvg(g.Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteLatency)
                                                                                            .SelectMany(a => a.Values),
                                                                                        attrWriteLatencyThreshold);
                                                        var sstableCountInfo = MaxMinAvg(g.Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount)
                                                                                            .SelectMany(a => a.Values),
                                                                                        attrSSTableThreshold);
                                                        var partitionSizeInfo = MaxMinAvg(g.Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.CompactedPartitionMax
                                                                                                        || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.PartitionLarge
                                                                                                        || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.RowLarge)
                                                                                            .SelectMany(a => a.Values),
                                                                                          attrPartitionSizeThreshold);
                                                        var tombstoneRatioInfo = MaxMinAvg(g.Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCellRatio)
                                                                                            .SelectMany(a => a.Values),
                                                                                           attrTombstoneRatioThreshold);
                                                        var weightedFactors = (new decimal[] { readLatencyInfo.Item2, writeLatencyInfo.Item2, sstableCountInfo.Item2, partitionSizeInfo.Item2, tombstoneRatioInfo.Item2 })
                                                                                .Where(f => f > 0);

                                                        return new
                                                        {                                                           
                                                            g.Key.Keyspace,
                                                            g.Key.TableViewIndex,
                                                            g.Key.DataCenter,
                                                            g.Key.Node,
                                                            Attribs = g.Select(a => new { a.Property, a.Values }).ToArray(),
                                                            ReadLatencyInfo = readLatencyInfo,
                                                            WriteLatencyInfo = writeLatencyInfo,
                                                            SSTableCountInfo = sstableCountInfo,
                                                            PartitionSizeInfo = partitionSizeInfo,
                                                            TombstoneRatioInfo = tombstoneRatioInfo,
                                                            Flagged = readLatencyInfo.Item1 || writeLatencyInfo.Item1 || sstableCountInfo.Item1 || partitionSizeInfo.Item1 || tombstoneRatioInfo.Item1,
                                                            MaxWeightedFactor = weightedFactors.DefaultIfEmpty().Max(),
                                                            AvgWeightedFactor = weightedFactors.DefaultIfEmpty().Average()
                                                        };
                                                    }
                                                   ).ToArray();
                var selectedTables = allNodeTableStatCollection
                                        .Where(i => i.Flagged)
                                        .Select(i => new
                                        {
                                            i.Keyspace,
                                            i.TableViewIndex,
                                            PK = i.TableViewIndex is ICQLTable cqlTbl ? string.Join(",", cqlTbl.PrimaryKeys) : null
                                        }).Distinct().ToArray();
                var commonKeys = selectedTables
                                    .Where(i => i.PK != null)
                                    .GroupBy(i => i.PK, i => i.TableViewIndex)
                                    .Where(i => i.Count() > 1)
                                    .SelectMany(g => g.Select(i => new { PK = g.Key, Table = i }))
                                    .ToArray();

                foreach (var dcnodetblAttrib in allNodeTableStatCollection)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();
                                       
                    if(selectedTables.Any(t => t.Keyspace.Equals(dcnodetblAttrib.Keyspace) && t.TableViewIndex.Equals(dcnodetblAttrib.TableViewIndex)))
                    {
                        var dataRow = this.Table.NewRow();

                        dataRow.SetField(ColumnNames.DataCenter, dcnodetblAttrib.DataCenter.Name);
                        dataRow.SetField(ColumnNames.NodeIPAddress, dcnodetblAttrib.Node.Id.NodeName());
                        dataRow.SetField(ColumnNames.KeySpace, dcnodetblAttrib.Keyspace.Name);
                        dataRow.SetField(ColumnNames.Table, dcnodetblAttrib.TableViewIndex.ReferenceName);
                        dataRow.SetField("CQL Type", dcnodetblAttrib.TableViewIndex.GetType().Name);

                        if(dcnodetblAttrib.ReadLatencyInfo.Item3 >= 0)
                            dataRow.SetField("Read Max", dcnodetblAttrib.ReadLatencyInfo.Item3);
                        if (dcnodetblAttrib.ReadLatencyInfo.Item4 >= 0)
                            dataRow.SetField("Read Min", dcnodetblAttrib.ReadLatencyInfo.Item4);
                        if (dcnodetblAttrib.ReadLatencyInfo.Item5 >= 0)
                            dataRow.SetField("Read Avg", dcnodetblAttrib.ReadLatencyInfo.Item5);
                        if (dcnodetblAttrib.ReadLatencyInfo.Item6 >= 0)
                            dataRow.SetField("Read StdDev", dcnodetblAttrib.ReadLatencyInfo.Item6);
                        if (dcnodetblAttrib.ReadLatencyInfo.Item2 >= 0)
                            dataRow.SetField("Read Factor", dcnodetblAttrib.ReadLatencyInfo.Item2);

                        if (dcnodetblAttrib.WriteLatencyInfo.Item3 >= 0)
                            dataRow.SetField("Write Max", dcnodetblAttrib.WriteLatencyInfo.Item3);
                        if (dcnodetblAttrib.WriteLatencyInfo.Item4 >= 0)
                            dataRow.SetField("Write Min", dcnodetblAttrib.WriteLatencyInfo.Item4);
                        if (dcnodetblAttrib.WriteLatencyInfo.Item5 >= 0)
                            dataRow.SetField("Write Avg", dcnodetblAttrib.WriteLatencyInfo.Item5);
                        if (dcnodetblAttrib.WriteLatencyInfo.Item6 >= 0)
                            dataRow.SetField("Write StdDev", dcnodetblAttrib.WriteLatencyInfo.Item6);
                        if (dcnodetblAttrib.WriteLatencyInfo.Item2 >= 0)
                            dataRow.SetField("Write Factor", dcnodetblAttrib.WriteLatencyInfo.Item2);

                        if (dcnodetblAttrib.SSTableCountInfo.Item3 >= 0)
                            dataRow.SetField("SSTables Max", (long) dcnodetblAttrib.SSTableCountInfo.Item3);
                        if (dcnodetblAttrib.SSTableCountInfo.Item4 >= 0)
                            dataRow.SetField("SSTables Min", (long)dcnodetblAttrib.SSTableCountInfo.Item4);
                        if (dcnodetblAttrib.SSTableCountInfo.Item5 >= 0)
                            dataRow.SetField("SSTables Avg", dcnodetblAttrib.SSTableCountInfo.Item5);
                        if (dcnodetblAttrib.SSTableCountInfo.Item6 >= 0)
                            dataRow.SetField("SSTables StdDev", dcnodetblAttrib.SSTableCountInfo.Item6);
                        if (dcnodetblAttrib.SSTableCountInfo.Item2 >= 0)
                            dataRow.SetField("SSTables Factor", dcnodetblAttrib.SSTableCountInfo.Item2);

                        if (dcnodetblAttrib.TombstoneRatioInfo.Item3 >= 0)
                            dataRow.SetField("Tombstone Ratio Max", dcnodetblAttrib.TombstoneRatioInfo.Item3);
                        if (dcnodetblAttrib.TombstoneRatioInfo.Item4 >= 0)
                            dataRow.SetField("Tombstone Ratio Min", dcnodetblAttrib.TombstoneRatioInfo.Item4);
                        if (dcnodetblAttrib.TombstoneRatioInfo.Item5 >= 0)
                            dataRow.SetField("Tombstone Ratio Avg", dcnodetblAttrib.TombstoneRatioInfo.Item5);
                        if (dcnodetblAttrib.TombstoneRatioInfo.Item6 >= 0)
                            dataRow.SetField("Tombstone Ratio StdDev", dcnodetblAttrib.TombstoneRatioInfo.Item6);
                        if (dcnodetblAttrib.TombstoneRatioInfo.Item2 >= 0)
                            dataRow.SetField("Tombstone Ratio Factor", dcnodetblAttrib.TombstoneRatioInfo.Item2);


                        {
                            var totTBRead = dcnodetblAttrib.Attribs
                                                .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstonesRead
                                                                || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxTombstonesSlice)
                                                .SelectMany(a => a.Values)
                                                .DefaultIfEmpty()
                                                .Sum();
                            if(totTBRead > 0)
                                dataRow.SetField("Tombstones Read", totTBRead);
                        }

                        {
                            var totLiveRead = dcnodetblAttrib.Attribs
                                                .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCell
                                                                || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxLiveCellsReadSlice)
                                                .SelectMany(a => a.Values)
                                                .DefaultIfEmpty()
                                                .Sum();

                            if (totLiveRead > 0)
                                dataRow.SetField("Live Read", totLiveRead);
                        }

                        if (dcnodetblAttrib.PartitionSizeInfo.Item3 >= 0)
                            dataRow.SetField("Partition Size Max", dcnodetblAttrib.PartitionSizeInfo.Item3);
                        if (dcnodetblAttrib.PartitionSizeInfo.Item4 >= 0)
                            dataRow.SetField("Partition Size Min", dcnodetblAttrib.PartitionSizeInfo.Item4);
                        if (dcnodetblAttrib.PartitionSizeInfo.Item5 >= 0)
                            dataRow.SetField("Partition Size Avg", dcnodetblAttrib.PartitionSizeInfo.Item5);
                        if (dcnodetblAttrib.PartitionSizeInfo.Item6 >= 0)
                            dataRow.SetField("Partition Size StdDev", dcnodetblAttrib.PartitionSizeInfo.Item6);
                        if (dcnodetblAttrib.PartitionSizeInfo.Item2 >= 0)
                            dataRow.SetField("Partition Size Factor", dcnodetblAttrib.PartitionSizeInfo.Item2);

                        /*dataRow.SetField("Flush Size Max", ;
                        dataRow.SetField("Flush Size Min", ;
                        dataRow.SetField("Flush Size Avg", ;
                        dataRow.SetField("Flush Size Factor", ;
                        dataRow.SetField("Flush Pending", ; */

                        if(totStorage > 0)
                        {
                            var localTot = dcnodetblAttrib.Attribs
                                            .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage)
                                            .SelectMany(a => a.Values)
                                            .DefaultIfEmpty()
                                            .Sum();
                            dataRow.SetField("Storage Percent", localTot / totStorage);                            
                        }
                        else
                            dataRow.SetField("Storage Percent", 0m);

                        if (totKeys > 0)
                        {
                            var localTot = dcnodetblAttrib.Attribs
                                            .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrKeys
                                                            || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrKeys1)
                                            .SelectMany(a => a.Values)
                                            .DefaultIfEmpty()
                                            .Sum();
                            dataRow.SetField("Keys Percent", localTot / totKeys);
                        }
                        else
                            dataRow.SetField("Keys Percent", 0m);

                        if (totReadCnt > 0)
                        {
                            var localTot = dcnodetblAttrib.Attribs
                                           .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount)
                                           .SelectMany(a => a.Values)
                                           .DefaultIfEmpty()
                                           .Sum();
                            dataRow.SetField("Read Percent", localTot / totReadCnt);
                        }
                        else
                            dataRow.SetField("Keys Percent", 0m);

                        if (totWriteCnt > 0)
                        {
                            var localTot = dcnodetblAttrib.Attribs
                                          .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                          .SelectMany(a => a.Values)
                                          .DefaultIfEmpty()
                                          .Sum();
                            dataRow.SetField("Write Percent", localTot / totWriteCnt);
                        }
                        else
                            dataRow.SetField("Write Percent",0m);

                        if (totSSTables > 0)
                        {
                            var localTot = dcnodetblAttrib.Attribs
                                         .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount)
                                         .SelectMany(a => a.Values)
                                         .DefaultIfEmpty()
                                         .Sum();
                            dataRow.SetField("SSTable Percent", localTot / totSSTables);
                        }
                        else
                            dataRow.SetField("SSTable Percent",0m);

                        if (dcnodetblAttrib.TableViewIndex is ICQLTable cqlTable)
                        {
                            var nbrCQLItems = dcnodetblAttrib.Keyspace.GetIndexes()
                                                .Where(i => i.Table == cqlTable)                                                
                                                .Count();
                            if (nbrCQLItems > 0)
                                dataRow.SetField("Secondary Indexes", nbrCQLItems);

                            nbrCQLItems = dcnodetblAttrib.Keyspace.GetViews()
                                                .Where(i => i.Table == cqlTable)                                                
                                                .Count();

                            if (nbrCQLItems > 0)
                                dataRow.SetField("Materialized Views", nbrCQLItems);
                        }
                        else if (dcnodetblAttrib.TableViewIndex is ICQLIndex cqlIdx)
                        {
                            var baseTbl = allNodeTableStatCollection.FirstOrDefault(i => i.TableViewIndex == cqlIdx.Table && i.Node == dcnodetblAttrib.Node);

                            dataRow.SetField("Base Table", cqlIdx.Table.Name);
                            dataRow.SetField("Base Table Weighted Factor Max", baseTbl?.MaxWeightedFactor ?? 0m);
                            dataRow.SetField("Base Table Weighted Factor Avg", baseTbl?.AvgWeightedFactor ?? 0m);
                        }
                        else if (dcnodetblAttrib.TableViewIndex is ICQLMaterializedView cqlMV)
                        {
                            var baseTbl = allNodeTableStatCollection.FirstOrDefault(i => i.TableViewIndex == cqlMV.Table && i.Node == dcnodetblAttrib.Node);

                            dataRow.SetField("Base Table", cqlMV.Table.Name);
                            dataRow.SetField("Base Table Weighted Factor Max", baseTbl?.MaxWeightedFactor ?? 0m);
                            dataRow.SetField("Base Table Weighted Factor Avg", baseTbl?.AvgWeightedFactor ?? 0m);
                        }

                        {
                            var commonKey = commonKeys.FirstOrDefault(i => i.Table == dcnodetblAttrib.TableViewIndex);

                            if(commonKey != null)
                                dataRow.SetField("Common Key", commonKey.PK);
                        }

                        dataRow.SetField("Weighted Factor Max", dcnodetblAttrib.MaxWeightedFactor);
                        dataRow.SetField("Weighted Factor Avg", dcnodetblAttrib.AvgWeightedFactor);
                        
                        this.Table.Rows.Add(dataRow);
                        ++nbrItems;
                    }                                   
                }
                
                Logger.Instance.InfoFormat("Loaded {0:###,###,##0} Tagged Stats", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Tagged Stats Canceled");
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
