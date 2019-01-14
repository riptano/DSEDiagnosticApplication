using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class CommonPartitionKeyDataTable : DataTableLoad
    {
        public CommonPartitionKeyDataTable(DSEDiagnosticLibrary.Cluster cluster, IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> commonPKStats, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.CommonPKStats = commonPKStats;
        }

        public IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> CommonPKStats { get; }

        public struct Columns
        {
            public const string PartitionKey = "Partition Key";
            public const string ReadCount = "Reads";
            public const string ReadStdDev = "Read StdDev";
            public const string ReadFactor = "Read Factor";
            public const string WriteCount = "Writes";
            public const string WriteStdDev = "Write StdDev";
            public const string WriteFactor = "Write Factor";
            public const string PartitionKeysCount = "Partition Keys";
            public const string PartitionKeysStdDev = "Partition Keys StdDev";
            public const string PartitionKeysFactor = "Partition Keys Factor";
            public const string Storage = "Storage";
            public const string Tables = "Across Tables";
            public const string Factor = "Factor";
        }

        public override DataTable CreateInitializationTable()
        {
            var dtPartitionInfo = new DataTable(TableNames.CommonPartitionKey, TableNames.Namespace);

            if (this.SessionId.HasValue) dtPartitionInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtPartitionInfo.Columns.Add(Columns.PartitionKey, typeof(string));
            dtPartitionInfo.Columns.Add(ColumnNames.DataCenter, typeof(string)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(ColumnNames.KeySpace, typeof(string)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.Tables, typeof(int)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.Factor, typeof(decimal));
            dtPartitionInfo.Columns.Add(Columns.ReadCount, typeof(long));
            dtPartitionInfo.Columns.Add(Columns.ReadStdDev, typeof(decimal)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.ReadFactor, typeof(decimal)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.WriteCount, typeof(long));
            dtPartitionInfo.Columns.Add(Columns.WriteStdDev, typeof(decimal)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.WriteFactor, typeof(decimal)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.PartitionKeysCount, typeof(long));
            dtPartitionInfo.Columns.Add(Columns.PartitionKeysStdDev, typeof(decimal)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.PartitionKeysFactor, typeof(decimal)).AllowDBNull = true;
            dtPartitionInfo.Columns.Add(Columns.Storage, typeof(decimal));
           
            dtPartitionInfo.DefaultView.ApplyDefaultSort = false;
            dtPartitionInfo.DefaultView.AllowDelete = false;
            dtPartitionInfo.DefaultView.AllowEdit = false;
            dtPartitionInfo.DefaultView.AllowNew = false;
            dtPartitionInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [{2}] ASC, [{3}] ASC",
                                                                Columns.PartitionKey,
                                                                ColumnNames.DataCenter,
                                                                ColumnNames.KeySpace,
                                                                ColumnNames.Table);

            return dtPartitionInfo;
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

                Logger.Instance.InfoFormat("Loading Common Partition Keys");

                var statTblReadsWrites = this.Cluster.Nodes.SelectMany(d => d.AggregatedStats.Where(i => i.Class.HasFlag(EventClasses.KeyspaceTableViewIndexStats | EventClasses.Node)))
                                        .Where(s => s.TableViewIndex != null
                                                        && s.Node != null
                                                        && s.TableViewIndex is ICQLTable)
                                        .Select(s => new
                                        {
                                            DC = s.DataCenter,
                                            s.Node,
                                            Table = s.TableViewIndex,
                                            LocalReads = (long) ((dynamic) s.Data.GetPropertyValue(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalReadCount) ?? 0L),
                                            LocalWrites = (long)((dynamic) s.Data.GetPropertyValue(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.LocalWriteCount) ?? 0L),
                                            PartitionKeys = (long)((dynamic)s.Data.GetPropertyValue(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.NbrPartitionKeys)
                                                                ?? 0L),
                                            Storage = (decimal) ((dynamic)s.Data.GetPropertyValue(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.TotalStorage)
                                                                ?? 0M)
                                        });

                var cpkStatsList = (new { PartitionKey = default(string),
                                            DataCenter = default(IDataCenter),
                                            Node = default(INode),
                                            Table = default(ICQLTable),
                                            Reads = default(long),
                                            Writes = default(long),
                                            PartitionKeys = default(long),
                                            Storage = default(decimal) }).CreateList();

                foreach (var cpkStat in this.CommonPKStats.Cast<DSEDiagnosticAnalytics.DataModelDCPK.CommonPartitionKey>())
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    var numSystemDSEKS = cpkStat.Tables.Where(i => i.Keyspace.IsSystemKeyspace || i.Keyspace.IsDSEKeyspace).Count();

                    if (cpkStat.Tables.Count() - numSystemDSEKS <= 1) continue;

                    foreach (var cpkTable in cpkStat.Tables)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if (cpkTable.Keyspace.IsDSEKeyspace || cpkTable.Keyspace.IsSystemKeyspace) continue;

                        var assocReadWriteTblNodesInfo = statTblReadsWrites.Where(i => i.Table == cpkTable);

                        if (!Properties.Settings.Default.CommonPartitionsAllowZeroReadWriteCnts)
                        {
                            var totalReads = assocReadWriteTblNodesInfo.DefaultIfEmpty().Sum(i => (dynamic)i.LocalReads);
                            var totalWrites = assocReadWriteTblNodesInfo.DefaultIfEmpty().Sum(i => (dynamic)i.LocalWrites);

                            if (totalReads == 0 && totalWrites == 0) continue;
                        }

                        foreach (var nodeInfo in assocReadWriteTblNodesInfo)
                        {
                            this.CancellationToken.ThrowIfCancellationRequested();

                            cpkStatsList.Add(new
                            {
                                PartitionKey = string.Join(",", cpkStat.PartitionKey.Select(k => k.Name + ' ' + k.CQLType.Name)),
                                DataCenter = nodeInfo.DC,
                                nodeInfo.Node,
                                Table = cpkTable,
                                Reads = nodeInfo.LocalReads,
                                Writes = nodeInfo.LocalWrites,
                                nodeInfo.PartitionKeys,
                                nodeInfo.Storage
                            });
                           
                            ++nbrItems;
                        }
                    }                    
                }

                this.CancellationToken.ThrowIfCancellationRequested();

                var attrCommonPartitionThreshold = DSEDiagnosticAnalytics.AttributeThreshold.FindAttrThreshold(DSEDiagnosticAnalytics.Properties.StatPropertyNames.Default.CommonPartitionKeys);

                var pkStdDevStats = from cpkStat in cpkStatsList
                                    group cpkStat by new { cpkStat.PartitionKey, cpkStat.Table, cpkStat.DataCenter } into grp
                                    //let RF = grp.Key.Table.Keyspace.Replications.Where(i => i.DataCenter == grp.Key.DataCenter).Select(i => i.RF).FirstOrDefault()
                                    //let DCNodes = grp.Key.DataCenter.Nodes.Count()
                                    let readStdDev = grp.Select(i => i.Reads).DefaultIfEmpty().StandardDeviationP()
                                    let writeStdDev = grp.Select(i => i.Writes).DefaultIfEmpty().StandardDeviationP()
                                    let pkStdDev = grp.Select(i => i.PartitionKeys).DefaultIfEmpty().StandardDeviationP()
                                    select new
                                    {
                                        grp.Key.PartitionKey,
                                        grp.Key.Table,
                                        grp.Key.DataCenter,
                                        Reads = grp.Select(i => i.Reads).DefaultIfEmpty().Sum(),
                                        ReadStdDev = readStdDev,
                                        Writes = grp.Select(i => i.Writes).DefaultIfEmpty().Sum(),
                                        WriteStdDev = writeStdDev,
                                        PartitionKeys = grp.Select(i => i.PartitionKeys).DefaultIfEmpty().Sum(),
                                        PartitionKeyStdDev = pkStdDev,
                                        Storage = grp.Select(i => i.Storage).DefaultIfEmpty().Sum(),
                                        Factor = attrCommonPartitionThreshold.DetermineWeightFactor( (decimal) (readStdDev + writeStdDev + pkStdDev) / 3m)
                                    };
                var pkFactorStats = from cpkStat in pkStdDevStats
                                    group cpkStat by new { cpkStat.PartitionKey } into grp                                    
                                    select new
                                    {
                                        grp.Key.PartitionKey,                                        
                                        Reads = grp.Select(i => i.Reads).DefaultIfEmpty().Sum(),
                                        ReadFactor = attrCommonPartitionThreshold.DetermineWeightFactor((decimal) grp.Select(i => i.ReadStdDev).DefaultIfEmpty().Max(),
                                                                                                        (decimal) grp.Select(i => i.ReadStdDev).DefaultIfEmpty().Min(),
                                                                                                        (decimal) grp.Select(i => i.ReadStdDev).DefaultIfEmpty().Average()),
                                        Writes = grp.Select(i => i.Writes).DefaultIfEmpty().Sum(),
                                        WriteFactor = attrCommonPartitionThreshold.DetermineWeightFactor((decimal)grp.Select(i => i.WriteStdDev).DefaultIfEmpty().Max(),
                                                                                                         (decimal)grp.Select(i => i.WriteStdDev).DefaultIfEmpty().Min(),
                                                                                                         (decimal)grp.Select(i => i.WriteStdDev).DefaultIfEmpty().Average()),
                                        PartitionKeys = grp.Select(i => i.PartitionKeys).DefaultIfEmpty().Sum(),
                                        PartitionKeyFactor = attrCommonPartitionThreshold.DetermineWeightFactor((decimal)grp.Select(i => i.PartitionKeyStdDev).DefaultIfEmpty().Max(),
                                                                                                                (decimal)grp.Select(i => i.PartitionKeyStdDev).DefaultIfEmpty().Min(),
                                                                                                                (decimal)grp.Select(i => i.PartitionKeyStdDev).DefaultIfEmpty().Average()),
                                        Storage = grp.Select(i => i.Storage).DefaultIfEmpty().Sum(),
                                        Tables = grp.Select(i => i.Table.FullName).Distinct().Count()
                                    };

                foreach (var statData in pkStdDevStats)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    dataRow = this.Table.NewRow();
                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetFieldStringLimit(Columns.PartitionKey, statData.PartitionKey);

                    dataRow.SetField(ColumnNames.DataCenter, statData.DataCenter.Name);
                    dataRow.SetField(ColumnNames.KeySpace, statData.Table.Keyspace.Name);
                    dataRow.SetField(ColumnNames.Table, statData.Table.Name);                    
                    dataRow.SetField(Columns.ReadCount, statData.Reads);
                    dataRow.SetField(Columns.ReadStdDev, statData.ReadStdDev);
                    dataRow.SetField(Columns.WriteCount, statData.Writes);
                    dataRow.SetField(Columns.WriteStdDev, statData.WriteStdDev);
                    dataRow.SetField(Columns.PartitionKeysCount, statData.PartitionKeys);
                    dataRow.SetField(Columns.PartitionKeysStdDev, statData.PartitionKeyStdDev);
                    dataRow.SetField(Columns.Storage, statData.Storage);
                    dataRow.SetField(Columns.Factor, statData.Factor);

                    this.Table.Rows.Add(dataRow);
                }

                foreach (var statData in pkFactorStats)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    dataRow = this.Table.NewRow();
                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetFieldStringLimit(Columns.PartitionKey, statData.PartitionKey);
                    
                    dataRow.SetField(Columns.ReadCount, statData.Reads);
                    dataRow.SetField(Columns.ReadFactor, statData.ReadFactor);
                    dataRow.SetField(Columns.WriteCount, statData.Writes);
                    dataRow.SetField(Columns.WriteFactor, statData.WriteFactor);
                    dataRow.SetField(Columns.PartitionKeysCount, statData.PartitionKeys);
                    dataRow.SetField(Columns.PartitionKeysFactor, statData.PartitionKeyFactor);
                    dataRow.SetField(Columns.Storage, statData.Storage);
                    dataRow.SetField(Columns.Tables, statData.Tables);
                    dataRow.SetField(Columns.Factor, (statData.ReadFactor + statData.WriteFactor + statData.PartitionKeyFactor) / 3M);

                    this.Table.Rows.Add(dataRow);
                }

                Logger.Instance.InfoFormat("Loaded Common Partition Keys, Total Nbr Items {0:###,###,##0}", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Common Partition Keys Canceled");
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
