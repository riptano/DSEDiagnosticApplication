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

        public TaggedItemsDataTable(DSEDiagnosticLibrary.Cluster cluster, DataTable commonKeyTable, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
            this.CommonKeyTable = commonKeyTable;
        }

        public string[] IgnoreKeySpaces { get; }
        public DataTable CommonKeyTable { get; }

        public struct Columns 
        {
            public const string CQLType = "CQL Type";            
            public const string CompactionStrategy = "Compaction Strategy";

            public const string ReadMax = "Read Max";
            public const string ReadMin = "Read Min";
            public const string ReadAvg = "Read Avg";
            public const string ReadStdDev = "Read StdDev";
            public const string ReadFactor = "Read Factor";
            public const string ReadPercent = "Read Percent";

            public const string WriteMax = "Write Max";
            public const string WriteMin = "Write Min";
            public const string WriteAvg = "Write Avg";
            public const string WriteStdDev = "Write StdDev";
            public const string WriteFactor = "Write Factor";

            public const string WritePercent = "Write Percent";
            public const string KeysPercent = "Keys Percent";

            public const string SSTablesMax = "SSTables Max";
            public const string SSTablesMin = "SSTables Min";
            public const string SSTablesAvg = "SSTables Avg";
            public const string SSTablesStdDev = "SSTables StdDev";
            public const string SSTablesFactor = "SSTables Factor";
            public const string SSTablePercent = "SSTable Percent";

            public const string StoragePercent = "Storage Percent";

            public const string TombstoneRatioMax = "Tombstone Ratio Max";
            public const string TombstoneRatioMin = "Tombstone Ratio Min";
            public const string TombstoneRatioAvg = "Tombstone Ratio Avg";
            public const string TombstoneRatioStdDev = "Tombstone Ratio StdDev";
            public const string TombstoneRatioFactor = "Tombstone Ratio Factor";
            public const string TombstonesRead = "Tombstones Read";
            public const string LiveRead = "Live Read";

            public const string PartitionSizeMax = "Partition Size Max";
            public const string PartitionSizeMin = "Partition Size Min";
            public const string PartitionSizeAvg = "Partition Size Avg";
            public const string PartitionSizeStdDev = "Partition Size StdDev";
            public const string PartitionSizeFactor = "Partition Size Factor";

            public const string FlushSizeMax = "Flush Size Max";
            public const string FlushSizeMin = "Flush Size Min";
            public const string FlushSizeAvg = "Flush Size Avg";
            public const string FlushSizeStdDev = "Flush Size StdDev";
            public const string FlushSizeFactor = "Flush Size Factor";
            public const string FlushPending = "Flush Pending";

            public const string WeightedFactorMax = "Weighted Factor Max";
            public const string WeightedFactorAvg = "Weighted Factor Avg";

            public const string SecondaryIndexes = "Secondary Indexes";
            public const string MaterializedViews = "Materialized Views";
            public const string CommonKey = "Common Key";
            public const string CommonKeyFactor = "Common Key Factor";

            public const string BaseTable = "Base Table";
            public const string BaseTableWeightedFactorMax = "Base Table Weighted Factor Max";
            public const string BaseTableWeightedFactorAvg = "Base Table Weighted Factor Avg";
        }

        public override DataTable CreateInitializationTable()
        {
            var dtStats = new DataTable(TableNames.TaggedItems, TableNames.Namespace);

            if (this.SessionId.HasValue) dtStats.Columns.Add(ColumnNames.SessionId, typeof(Guid));
            
           
            dtStats.Columns.Add(ColumnNames.KeySpace, typeof(string));
            dtStats.Columns.Add(Columns.CompactionStrategy, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add(ColumnNames.Table, typeof(string));
            dtStats.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtStats.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.CQLType, typeof(string));

            dtStats.Columns.Add(Columns.WeightedFactorMax, typeof(decimal));
            dtStats.Columns.Add(Columns.WeightedFactorAvg, typeof(decimal));

            dtStats.Columns.Add(Columns.ReadMax, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.ReadMin, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.ReadAvg, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.ReadStdDev, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.ReadFactor, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.ReadPercent, typeof(decimal)).AllowDBNull = true;
           
            dtStats.Columns.Add(Columns.WriteMax, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.WriteMin, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.WriteAvg, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.WriteStdDev, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.WriteFactor, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.WritePercent, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.KeysPercent, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.SSTablesMax, typeof(long)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.SSTablesMin, typeof(long)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.SSTablesAvg, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.SSTablesStdDev, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.SSTablesFactor, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.SSTablePercent, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.StoragePercent, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.TombstoneRatioMax, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.TombstoneRatioMin, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.TombstoneRatioAvg, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.TombstoneRatioStdDev, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.TombstoneRatioFactor, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.TombstonesRead, typeof(long)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.LiveRead, typeof(long)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.PartitionSizeMax, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.PartitionSizeMin, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.PartitionSizeAvg, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.PartitionSizeStdDev, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.PartitionSizeFactor, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.FlushSizeMax, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.FlushSizeMin, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.FlushSizeAvg, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.FlushSizeStdDev, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.FlushSizeFactor, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.FlushPending, typeof(long)).AllowDBNull = true;
                        
            dtStats.Columns.Add(Columns.SecondaryIndexes, typeof(int)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.MaterializedViews, typeof(int)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.CommonKey, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.CommonKeyFactor, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(Columns.BaseTable, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.BaseTableWeightedFactorMax, typeof(decimal)).AllowDBNull = true;
            dtStats.Columns.Add(Columns.BaseTableWeightedFactorAvg, typeof(decimal)).AllowDBNull = true;

            dtStats.Columns.Add(ColumnNames.ReconciliationRef, typeof(object)).AllowDBNull = true;

            dtStats.DefaultView.ApplyDefaultSort = false;
            dtStats.DefaultView.AllowDelete = false;
            dtStats.DefaultView.AllowEdit = false;
            dtStats.DefaultView.AllowNew = false;
            dtStats.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [{2}] ASC, [{3}] ASC, [{4}] ASC",
                                                        ColumnNames.KeySpace,
                                                        Columns.CompactionStrategy,
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
                                                                                                                            || DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(a.Key)
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadLatency
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteLatency
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxTombstonesSlice
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstonesRead
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCell
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxLiveCellsReadSlice
                                                                                                                            || a.Key == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCellRatio
                                                                                                                            || DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.PartitionRowLarge.ComparePropName(a.Key))
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
                                    .Select(a => a.Values.DefaultIfEmpty().Sum())
                                    .DefaultIfEmpty()
                                    .Sum();
                var totKeys = allStatCollection
                                .Where(i => DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(i.Property))
                                .Select(a => a.Values.DefaultIfEmpty().Sum())
                                .DefaultIfEmpty()
                                .Sum();
                var totReadCnt = allStatCollection
                                    .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount)
                                    .Select(a => a.Values.DefaultIfEmpty().Sum())
                                    .DefaultIfEmpty()
                                    .Sum();
                var totWriteCnt = allStatCollection
                                    .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                    .Select(a => a.Values.DefaultIfEmpty().Sum())
                                    .DefaultIfEmpty()
                                    .Sum();
                var totSSTables = allStatCollection
                                    .Where(i => i.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount)
                                    .Select(a => a.Values.DefaultIfEmpty().Sum())
                                    .DefaultIfEmpty()
                                    .Sum();

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

                //Detail
                {
                    var allNodeTableStatCollection = allStatCollection
                                                        .Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace && !i.Keyspace.IsPerformanceKeyspace)
                                                        .GroupBy(i => new { i.Keyspace, i.TableViewIndex, i.DataCenter, i.Node })
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
                                                            var partitionSizeInfo = MaxMinAvg(g.Where(a => DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.PartitionRowLarge.ComparePropName(a.Property))
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
                                            }).Distinct().ToArray();
                    var commonKeys = (from dataRow in this.CommonKeyTable.AsEnumerable()
                                      where !dataRow.IsNull(ColumnNames.Table)
                                      let ksName = dataRow.Field<string>(ColumnNames.KeySpace)
                                      let tblName = dataRow.Field<string>(ColumnNames.Table)
                                      let tblInstance = selectedTables.FirstOrDefault(i => i.Keyspace.Equals(ksName) && i.TableViewIndex.Equals(tblName))?.TableViewIndex
                                      where tblInstance != null
                                      let pk = dataRow.Field<string>(CommonPartitionKeyDataTable.Columns.PartitionKey)
                                      let dcName = dataRow.Field<string>(ColumnNames.DataCenter)
                                      let factor = dataRow.Field<decimal>(CommonPartitionKeyDataTable.Columns.Factor)
                                      select new
                                      {
                                          PartitionKey = pk,
                                          DCName = dcName,
                                          TblInstance = tblInstance,
                                          Factor = factor
                                      }).ToArray();

                    foreach (var dcnodetblAttrib in allNodeTableStatCollection)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if (selectedTables.Any(t => t.Keyspace.Equals(dcnodetblAttrib.Keyspace) && t.TableViewIndex.Equals(dcnodetblAttrib.TableViewIndex)))
                        {
                            var dataRow = this.Table.NewRow();

                            dataRow.SetField(ColumnNames.DataCenter, dcnodetblAttrib.DataCenter.Name);
                            dataRow.SetField(ColumnNames.NodeIPAddress, dcnodetblAttrib.Node.NodeName());
                            dataRow.SetField(ColumnNames.KeySpace, dcnodetblAttrib.Keyspace.Name);

                            if (dcnodetblAttrib.TableViewIndex is ICQLTable tableInstance)
                                dataRow.SetField(Columns.CompactionStrategy, tableInstance.Compaction);

                            dataRow.SetField(ColumnNames.Table, dcnodetblAttrib.TableViewIndex.ReferenceNameWAttrs());
                            dataRow.SetField(Columns.CQLType, dcnodetblAttrib.TableViewIndex.GetType().Name);

                            dataRow.SetField(Columns.WeightedFactorMax, dcnodetblAttrib.MaxWeightedFactor);
                            dataRow.SetField(Columns.WeightedFactorAvg, dcnodetblAttrib.AvgWeightedFactor);

                            if (dcnodetblAttrib.ReadLatencyInfo.Item3 >= 0)
                                dataRow.SetField(Columns.ReadMax, dcnodetblAttrib.ReadLatencyInfo.Item3);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item4 >= 0)
                                dataRow.SetField(Columns.ReadMin, dcnodetblAttrib.ReadLatencyInfo.Item4);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item5 >= 0)
                                dataRow.SetField(Columns.ReadAvg, dcnodetblAttrib.ReadLatencyInfo.Item5);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item6 >= 0)
                                dataRow.SetField(Columns.ReadStdDev, dcnodetblAttrib.ReadLatencyInfo.Item6);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item2 >= 0)
                                dataRow.SetField(Columns.ReadFactor, dcnodetblAttrib.ReadLatencyInfo.Item2);

                            if (dcnodetblAttrib.WriteLatencyInfo.Item3 >= 0)
                                dataRow.SetField(Columns.WriteMax, dcnodetblAttrib.WriteLatencyInfo.Item3);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item4 >= 0)
                                dataRow.SetField(Columns.WriteMin, dcnodetblAttrib.WriteLatencyInfo.Item4);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item5 >= 0)
                                dataRow.SetField(Columns.WriteAvg, dcnodetblAttrib.WriteLatencyInfo.Item5);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item6 >= 0)
                                dataRow.SetField(Columns.WriteStdDev, dcnodetblAttrib.WriteLatencyInfo.Item6);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item2 >= 0)
                                dataRow.SetField(Columns.WriteFactor, dcnodetblAttrib.WriteLatencyInfo.Item2);

                            if (dcnodetblAttrib.SSTableCountInfo.Item3 >= 0)
                                dataRow.SetField(Columns.SSTablesMax, (long)dcnodetblAttrib.SSTableCountInfo.Item3);
                            if (dcnodetblAttrib.SSTableCountInfo.Item4 >= 0)
                                dataRow.SetField(Columns.SSTablesMin, (long)dcnodetblAttrib.SSTableCountInfo.Item4);
                            if (dcnodetblAttrib.SSTableCountInfo.Item5 >= 0)
                                dataRow.SetField(Columns.SSTablesAvg, dcnodetblAttrib.SSTableCountInfo.Item5);
                            if (dcnodetblAttrib.SSTableCountInfo.Item6 >= 0)
                                dataRow.SetField(Columns.SSTablesStdDev, dcnodetblAttrib.SSTableCountInfo.Item6);
                            if (dcnodetblAttrib.SSTableCountInfo.Item2 >= 0)
                                dataRow.SetField(Columns.SSTablesFactor, dcnodetblAttrib.SSTableCountInfo.Item2);

                            if (dcnodetblAttrib.TombstoneRatioInfo.Item3 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioMax, dcnodetblAttrib.TombstoneRatioInfo.Item3);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item4 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioMin, dcnodetblAttrib.TombstoneRatioInfo.Item4);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item5 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioAvg, dcnodetblAttrib.TombstoneRatioInfo.Item5);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item6 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioStdDev, dcnodetblAttrib.TombstoneRatioInfo.Item6);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item2 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioFactor, dcnodetblAttrib.TombstoneRatioInfo.Item2);


                            {
                                var totTBRead = dcnodetblAttrib.Attribs
                                                    .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstonesRead
                                                                    || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxTombstonesSlice)
                                                    .SelectMany(a => a.Values)
                                                    .DefaultIfEmpty()
                                                    .Sum();
                                if (totTBRead > 0)
                                    dataRow.SetField(Columns.TombstonesRead, totTBRead);
                            }

                            {
                                var totLiveRead = dcnodetblAttrib.Attribs
                                                    .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCell
                                                                    || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxLiveCellsReadSlice)
                                                    .SelectMany(a => a.Values)
                                                    .DefaultIfEmpty()
                                                    .Sum();

                                if (totLiveRead > 0)
                                    dataRow.SetField(Columns.LiveRead, totLiveRead);
                            }

                            if (dcnodetblAttrib.PartitionSizeInfo.Item3 >= 0)
                                dataRow.SetField(Columns.PartitionSizeMax, dcnodetblAttrib.PartitionSizeInfo.Item3);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item4 >= 0)
                                dataRow.SetField(Columns.PartitionSizeMin, dcnodetblAttrib.PartitionSizeInfo.Item4);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item5 >= 0)
                                dataRow.SetField(Columns.PartitionSizeAvg, dcnodetblAttrib.PartitionSizeInfo.Item5);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item6 >= 0)
                                dataRow.SetField(Columns.PartitionSizeStdDev, dcnodetblAttrib.PartitionSizeInfo.Item6);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item2 >= 0)
                                dataRow.SetField(Columns.PartitionSizeFactor, dcnodetblAttrib.PartitionSizeInfo.Item2);

                            /*dataRow.SetField(Columns.FlushSizeMax, ;
                            dataRow.SetField(Columns.FlushSizeMin, ;
                            dataRow.SetField(Columns.FlushSizeAvg, ;
                            dataRow.SetField(Columns.FlushSizeFactor, ;
                            dataRow.SetField(Columns.FlushPending, ; */

                            if (totStorage > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                                .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage)
                                                .SelectMany(a => a.Values)
                                                .DefaultIfEmpty()
                                                .Sum();
                                dataRow.SetField(Columns.StoragePercent, localTot / totStorage);
                            }
                            else
                                dataRow.SetField(Columns.StoragePercent, 0m);

                            if (totKeys > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                                .Where(a => DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(a.Property))
                                                .SelectMany(a => a.Values)
                                                .DefaultIfEmpty()
                                                .Sum();
                                dataRow.SetField(Columns.KeysPercent, localTot / totKeys);
                            }
                            else
                                dataRow.SetField(Columns.KeysPercent, 0m);

                            if (totReadCnt > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                               .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount)
                                               .SelectMany(a => a.Values)
                                               .DefaultIfEmpty()
                                               .Sum();
                                dataRow.SetField(Columns.ReadPercent, localTot / totReadCnt);
                            }
                            else
                                dataRow.SetField(Columns.KeysPercent, 0m);

                            if (totWriteCnt > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                              .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                              .SelectMany(a => a.Values)
                                              .DefaultIfEmpty()
                                              .Sum();
                                dataRow.SetField(Columns.WritePercent, localTot / totWriteCnt);
                            }
                            else
                                dataRow.SetField(Columns.WritePercent, 0m);

                            if (totSSTables > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                             .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount)
                                             .SelectMany(a => a.Values)
                                             .DefaultIfEmpty()
                                             .Sum();
                                dataRow.SetField(Columns.SSTablePercent, localTot / totSSTables);
                            }
                            else
                                dataRow.SetField(Columns.SSTablePercent, 0m);

                            if (dcnodetblAttrib.TableViewIndex is ICQLTable cqlTable)
                            {
                                var nbrCQLItems = dcnodetblAttrib.Keyspace.GetIndexes()
                                                    .Where(i => i.Table == cqlTable)
                                                    .Count();
                                if (nbrCQLItems > 0)
                                    dataRow.SetField(Columns.SecondaryIndexes, nbrCQLItems);

                                nbrCQLItems = dcnodetblAttrib.Keyspace.GetViews()
                                                    .Where(i => i.Table == cqlTable)
                                                    .Count();

                                if (nbrCQLItems > 0)
                                    dataRow.SetField(Columns.MaterializedViews, nbrCQLItems);
                            }
                            else if (dcnodetblAttrib.TableViewIndex is ICQLIndex cqlIdx)
                            {
                                var baseTbl = allNodeTableStatCollection.FirstOrDefault(i => i.TableViewIndex == cqlIdx.Table && i.Node == dcnodetblAttrib.Node);

                                dataRow.SetField(Columns.BaseTable, cqlIdx.Table.Name);
                                dataRow.SetField(Columns.BaseTableWeightedFactorMax, baseTbl?.MaxWeightedFactor ?? 0m);
                                dataRow.SetField(Columns.BaseTableWeightedFactorAvg, baseTbl?.AvgWeightedFactor ?? 0m);
                            }
                            else if (dcnodetblAttrib.TableViewIndex is ICQLMaterializedView cqlMV)
                            {
                                var baseTbl = allNodeTableStatCollection.FirstOrDefault(i => i.TableViewIndex == cqlMV.Table && i.Node == dcnodetblAttrib.Node);

                                dataRow.SetField(Columns.BaseTable, cqlMV.Table.Name);
                                dataRow.SetField(Columns.BaseTableWeightedFactorMax, baseTbl?.MaxWeightedFactor ?? 0m);
                                dataRow.SetField(Columns.BaseTableWeightedFactorAvg, baseTbl?.AvgWeightedFactor ?? 0m);
                            }

                            {
                                var commonKey = commonKeys.FirstOrDefault(i => i.TblInstance == dcnodetblAttrib.TableViewIndex
                                                                                && i.DCName == dcnodetblAttrib.DataCenter.Name);

                                if (commonKey != null)
                                {
                                    dataRow.SetField(Columns.CommonKey, commonKey.PartitionKey);
                                    dataRow.SetField(Columns.CommonKeyFactor, commonKey.Factor);
                                }
                            }

                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;
                        }
                    }
                }

                //DataCenter
                {
                    var allNodeTableStatCollection = allStatCollection
                                                        .Where(i => !i.Keyspace.IsSystemKeyspace && !i.Keyspace.IsDSEKeyspace && !i.Keyspace.IsPerformanceKeyspace)
                                                        .GroupBy(i => new { i.Keyspace, i.TableViewIndex, i.DataCenter })
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
                                                            var partitionSizeInfo = MaxMinAvg(g.Where(a => DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.PartitionRowLarge.ComparePropName(a.Property))
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
                                            }).Distinct().ToArray();
                    var commonKeys = (from dataRow in this.CommonKeyTable.AsEnumerable()
                                      where !dataRow.IsNull(ColumnNames.Table)
                                      let ksName = dataRow.Field<string>(ColumnNames.KeySpace)
                                      let tblName = dataRow.Field<string>(ColumnNames.Table)
                                      let tblInstance = selectedTables.FirstOrDefault(i => i.Keyspace.Equals(ksName) && i.TableViewIndex.Equals(tblName))?.TableViewIndex
                                      where tblInstance != null
                                      let pk = dataRow.Field<string>(CommonPartitionKeyDataTable.Columns.PartitionKey)
                                      let dcName = dataRow.Field<string>(ColumnNames.DataCenter)
                                      let factor = dataRow.Field<decimal>(CommonPartitionKeyDataTable.Columns.Factor)
                                      select new
                                      {
                                          PartitionKey = pk,
                                          DCName = dcName,
                                          TblInstance = tblInstance,
                                          Factor = factor
                                      }).ToArray();

                    foreach (var dcnodetblAttrib in allNodeTableStatCollection)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if (selectedTables.Any(t => t.Keyspace.Equals(dcnodetblAttrib.Keyspace) && t.TableViewIndex.Equals(dcnodetblAttrib.TableViewIndex)))
                        {
                            var dataRow = this.Table.NewRow();

                            dataRow.SetField(ColumnNames.DataCenter, dcnodetblAttrib.DataCenter.Name);                            
                            dataRow.SetField(ColumnNames.KeySpace, dcnodetblAttrib.Keyspace.Name);

                            if (dcnodetblAttrib.TableViewIndex is ICQLTable tableInstance)
                                dataRow.SetField(Columns.CompactionStrategy, tableInstance.Compaction);

                            dataRow.SetField(ColumnNames.Table, dcnodetblAttrib.TableViewIndex.ReferenceNameWAttrs());
                            dataRow.SetField(Columns.CQLType, dcnodetblAttrib.TableViewIndex.GetType().Name);

                            dataRow.SetField(Columns.WeightedFactorMax, dcnodetblAttrib.MaxWeightedFactor);
                            dataRow.SetField(Columns.WeightedFactorAvg, dcnodetblAttrib.AvgWeightedFactor);

                            if (dcnodetblAttrib.ReadLatencyInfo.Item3 >= 0)
                                dataRow.SetField(Columns.ReadMax, dcnodetblAttrib.ReadLatencyInfo.Item3);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item4 >= 0)
                                dataRow.SetField(Columns.ReadMin, dcnodetblAttrib.ReadLatencyInfo.Item4);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item5 >= 0)
                                dataRow.SetField(Columns.ReadAvg, dcnodetblAttrib.ReadLatencyInfo.Item5);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item6 >= 0)
                                dataRow.SetField(Columns.ReadStdDev, dcnodetblAttrib.ReadLatencyInfo.Item6);
                            if (dcnodetblAttrib.ReadLatencyInfo.Item2 >= 0)
                                dataRow.SetField(Columns.ReadFactor, dcnodetblAttrib.ReadLatencyInfo.Item2);

                            if (dcnodetblAttrib.WriteLatencyInfo.Item3 >= 0)
                                dataRow.SetField(Columns.WriteMax, dcnodetblAttrib.WriteLatencyInfo.Item3);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item4 >= 0)
                                dataRow.SetField(Columns.WriteMin, dcnodetblAttrib.WriteLatencyInfo.Item4);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item5 >= 0)
                                dataRow.SetField(Columns.WriteAvg, dcnodetblAttrib.WriteLatencyInfo.Item5);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item6 >= 0)
                                dataRow.SetField(Columns.WriteStdDev, dcnodetblAttrib.WriteLatencyInfo.Item6);
                            if (dcnodetblAttrib.WriteLatencyInfo.Item2 >= 0)
                                dataRow.SetField(Columns.WriteFactor, dcnodetblAttrib.WriteLatencyInfo.Item2);

                            if (dcnodetblAttrib.SSTableCountInfo.Item3 >= 0)
                                dataRow.SetField(Columns.SSTablesMax, (long)dcnodetblAttrib.SSTableCountInfo.Item3);
                            if (dcnodetblAttrib.SSTableCountInfo.Item4 >= 0)
                                dataRow.SetField(Columns.SSTablesMin, (long)dcnodetblAttrib.SSTableCountInfo.Item4);
                            if (dcnodetblAttrib.SSTableCountInfo.Item5 >= 0)
                                dataRow.SetField(Columns.SSTablesAvg, dcnodetblAttrib.SSTableCountInfo.Item5);
                            if (dcnodetblAttrib.SSTableCountInfo.Item6 >= 0)
                                dataRow.SetField(Columns.SSTablesStdDev, dcnodetblAttrib.SSTableCountInfo.Item6);
                            if (dcnodetblAttrib.SSTableCountInfo.Item2 >= 0)
                                dataRow.SetField(Columns.SSTablesFactor, dcnodetblAttrib.SSTableCountInfo.Item2);

                            if (dcnodetblAttrib.TombstoneRatioInfo.Item3 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioMax, dcnodetblAttrib.TombstoneRatioInfo.Item3);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item4 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioMin, dcnodetblAttrib.TombstoneRatioInfo.Item4);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item5 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioAvg, dcnodetblAttrib.TombstoneRatioInfo.Item5);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item6 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioStdDev, dcnodetblAttrib.TombstoneRatioInfo.Item6);
                            if (dcnodetblAttrib.TombstoneRatioInfo.Item2 >= 0)
                                dataRow.SetField(Columns.TombstoneRatioFactor, dcnodetblAttrib.TombstoneRatioInfo.Item2);


                            {
                                var totTBRead = dcnodetblAttrib.Attribs
                                                    .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstonesRead
                                                                    || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxTombstonesSlice)
                                                    .SelectMany(a => a.Values)
                                                    .DefaultIfEmpty()
                                                    .Sum();
                                if (totTBRead > 0)
                                    dataRow.SetField(Columns.TombstonesRead, totTBRead);
                            }

                            {
                                var totLiveRead = dcnodetblAttrib.Attribs
                                                    .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TombstoneLiveCell
                                                                    || a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.MaxLiveCellsReadSlice)
                                                    .SelectMany(a => a.Values)
                                                    .DefaultIfEmpty()
                                                    .Sum();

                                if (totLiveRead > 0)
                                    dataRow.SetField(Columns.LiveRead, totLiveRead);
                            }

                            if (dcnodetblAttrib.PartitionSizeInfo.Item3 >= 0)
                                dataRow.SetField(Columns.PartitionSizeMax, dcnodetblAttrib.PartitionSizeInfo.Item3);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item4 >= 0)
                                dataRow.SetField(Columns.PartitionSizeMin, dcnodetblAttrib.PartitionSizeInfo.Item4);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item5 >= 0)
                                dataRow.SetField(Columns.PartitionSizeAvg, dcnodetblAttrib.PartitionSizeInfo.Item5);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item6 >= 0)
                                dataRow.SetField(Columns.PartitionSizeStdDev, dcnodetblAttrib.PartitionSizeInfo.Item6);
                            if (dcnodetblAttrib.PartitionSizeInfo.Item2 >= 0)
                                dataRow.SetField(Columns.PartitionSizeFactor, dcnodetblAttrib.PartitionSizeInfo.Item2);

                            /*dataRow.SetField(Columns.FlushSizeMax, ;
                            dataRow.SetField(Columns.FlushSizeMin, ;
                            dataRow.SetField(Columns.FlushSizeAvg, ;
                            dataRow.SetField(Columns.FlushSizeFactor, ;
                            dataRow.SetField(Columns.FlushPending, ; */

                            if (totStorage > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                                .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage)
                                                .SelectMany(a => a.Values)
                                                .DefaultIfEmpty()
                                                .Sum();
                                dataRow.SetField(Columns.StoragePercent, localTot / totStorage);
                            }
                            else
                                dataRow.SetField(Columns.StoragePercent, 0m);

                            if (totKeys > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                                .Where(a => DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrPartitionKeys.ComparePropName(a.Property))
                                                .SelectMany(a => a.Values)
                                                .DefaultIfEmpty()
                                                .Sum();
                                dataRow.SetField(Columns.KeysPercent, localTot / totKeys);
                            }
                            else
                                dataRow.SetField(Columns.KeysPercent, 0m);

                            if (totReadCnt > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                               .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount)
                                               .SelectMany(a => a.Values)
                                               .DefaultIfEmpty()
                                               .Sum();
                                dataRow.SetField(Columns.ReadPercent, localTot / totReadCnt);
                            }
                            else
                                dataRow.SetField(Columns.KeysPercent, 0m);

                            if (totWriteCnt > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                              .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount)
                                              .SelectMany(a => a.Values)
                                              .DefaultIfEmpty()
                                              .Sum();
                                dataRow.SetField(Columns.WritePercent, localTot / totWriteCnt);
                            }
                            else
                                dataRow.SetField(Columns.WritePercent, 0m);

                            if (totSSTables > 0)
                            {
                                var localTot = dcnodetblAttrib.Attribs
                                             .Where(a => a.Property == DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.SSTableCount)
                                             .SelectMany(a => a.Values)
                                             .DefaultIfEmpty()
                                             .Sum();
                                dataRow.SetField(Columns.SSTablePercent, localTot / totSSTables);
                            }
                            else
                                dataRow.SetField(Columns.SSTablePercent, 0m);

                            if (dcnodetblAttrib.TableViewIndex is ICQLTable cqlTable)
                            {
                                var nbrCQLItems = dcnodetblAttrib.Keyspace.GetIndexes()
                                                    .Where(i => i.Table == cqlTable)
                                                    .Count();
                                if (nbrCQLItems > 0)
                                    dataRow.SetField(Columns.SecondaryIndexes, nbrCQLItems);

                                nbrCQLItems = dcnodetblAttrib.Keyspace.GetViews()
                                                    .Where(i => i.Table == cqlTable)
                                                    .Count();

                                if (nbrCQLItems > 0)
                                    dataRow.SetField(Columns.MaterializedViews, nbrCQLItems);
                            }
                            else if (dcnodetblAttrib.TableViewIndex is ICQLIndex cqlIdx)
                            {
                                var baseTbl = allNodeTableStatCollection.FirstOrDefault(i => i.TableViewIndex == cqlIdx.Table);

                                dataRow.SetField(Columns.BaseTable, cqlIdx.Table.Name);
                                dataRow.SetField(Columns.BaseTableWeightedFactorMax, baseTbl?.MaxWeightedFactor ?? 0m);
                                dataRow.SetField(Columns.BaseTableWeightedFactorAvg, baseTbl?.AvgWeightedFactor ?? 0m);
                            }
                            else if (dcnodetblAttrib.TableViewIndex is ICQLMaterializedView cqlMV)
                            {
                                var baseTbl = allNodeTableStatCollection.FirstOrDefault(i => i.TableViewIndex == cqlMV.Table);

                                dataRow.SetField(Columns.BaseTable, cqlMV.Table.Name);
                                dataRow.SetField(Columns.BaseTableWeightedFactorMax, baseTbl?.MaxWeightedFactor ?? 0m);
                                dataRow.SetField(Columns.BaseTableWeightedFactorAvg, baseTbl?.AvgWeightedFactor ?? 0m);
                            }

                            {
                                var commonKey = commonKeys.FirstOrDefault(i => i.TblInstance == dcnodetblAttrib.TableViewIndex
                                                                                && i.DCName == dcnodetblAttrib.DataCenter.Name);

                                if (commonKey != null)
                                {
                                    dataRow.SetField(Columns.CommonKey, commonKey.PartitionKey);
                                    dataRow.SetField(Columns.CommonKeyFactor, commonKey.Factor);
                                }
                            }

                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;
                        }
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
