using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class KeyspaceDataTable : DataTableLoad
    {
        public KeyspaceDataTable(DSEDiagnosticLibrary.Cluster cluster,
                                    CancellationTokenSource cancellationSource = null,
                                    string[] ignoreKeySpaces = null,
                                    string[] whitelistKeySpaces = null,
                                    Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
            this.WhitelistKeySpaces = whitelistKeySpaces ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }
        public string[] WhitelistKeySpaces { get; }

        public struct Columns
        {
            public const string Name = "Name";
            public const string ReplicationStrategy = "Replication Strategy";
            public const string ReplicationFactor = "Replication Factor";
            public const string Tables = "Tables";
            public const string Views = "Views";
            public const string SecondaryIndexes = "Secondary Indexes";
            public const string SolrIndexes = "solr Indexes";
            public const string SASIndexes = "SAS Indexes";
            public const string CustomIndexes = "Custom Indexes";
            public const string Functions = "Functions";
            public const string Triggers = "Triggers";
            public const string USTs = "User Defined Types";
            public const string Total = "Total";
            public const string Active = "Active";
            public const string ActivePercent = "Active Percent";
            public const string STCS = "STCS";
            public const string LCS = "LCS";
            public const string DTCS = "DTCS";
            public const string TCS = "TCS";
            public const string TWCS = "TWCS";
            public const string OtherStrategies = "Other Strategies";
            public const string ColumnTotal = "Column Total";
            public const string ColumnCollections = "Column Collections";
            public const string ColumnBlobs = "Column Blobs";
            public const string ColumnStatic = "Column Static";
            public const string ColumnFrozen = "Column Frozen";
            public const string ColumnTuple = "Column Tuple";
            public const string ColumnUDT = "Column UDT";
            public const string ReadRepairChance = "Read-Repair Chance (Max)";
            public const string ReadRepairDCChance = "Read-Repair DC Chance";
            public const string GCGrace = "GC Grace (Max)";
            public const string TTL = "TTL (Max)";
            public const string OrderBy = "OrderBy";
            public const string CompactStorage = "Compact Storage";
            public const string Storage = "Storage (MB)";
            public const string StorageUtilized = "Storage Utilized";
            public const string ReadPercent = "Read Percent";
            public const string WritePercent = "Write Percent";
            public const string PartitionKeys = "Partition Keys";
            public const string PartitionKeyPercent = "Key Percent";
            public const string SSTables = "SSTable";
            public const string SSTablePercent = "SSTable Percent";
            public const string FlushPercent = "Flush Percent";
            public const string CompactionPercent = "Compaction Percent";
            public const string RepairPercent = "Repair Percent";
            public const string TblsNodeSync = "Node Sync";
            public const string DDL = "DDL";
        }

        public override DataTable CreateInitializationTable()
        {
            var dtKeySpace = new DataTable(TableNames.Keyspaces, TableNames.Namespace);
            dtKeySpace.CaseSensitive();

            if (this.SessionId.HasValue) dtKeySpace.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtKeySpace.Columns.Add(Columns.Name, typeof(string));
            dtKeySpace.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtKeySpace.Columns.Add(Columns.ReplicationStrategy, typeof(string));            
            dtKeySpace.Columns.Add(Columns.ReplicationFactor, typeof(int));

            dtKeySpace.Columns.Add(Columns.Tables, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.Views, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.SecondaryIndexes, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.SolrIndexes, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.SASIndexes, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.CustomIndexes, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.Functions, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.Triggers, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.USTs, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.Total, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.Active, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ActivePercent, typeof(decimal)).AllowDBNull = true;

            dtKeySpace.Columns.Add(Columns.STCS, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.LCS, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.DTCS, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.TCS, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.TWCS, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.OtherStrategies, typeof(int)).AllowDBNull = true;

            dtKeySpace.Columns.Add(Columns.ColumnTotal, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ColumnCollections, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ColumnBlobs, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ColumnStatic, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ColumnFrozen, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ColumnTuple, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ColumnUDT, typeof(int)).AllowDBNull = true;

            dtKeySpace.Columns.Add(Columns.ReadRepairChance, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ReadRepairDCChance, typeof(decimal)).AllowDBNull = true;
            //dtKeySpace.Columns.Add(Columns.ReadRepairPolicy", typeof(string)).AllowDBNull = true;

            dtKeySpace.Columns.Add(Columns.GCGrace, typeof(TimeSpan)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.TTL, typeof(TimeSpan)).AllowDBNull = true;

            dtKeySpace.Columns.Add(Columns.OrderBy, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.CompactStorage, typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.TblsNodeSync, typeof(int)).AllowDBNull = true;

            dtKeySpace.Columns.Add(Columns.Storage, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.StorageUtilized, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.ReadPercent, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.WritePercent, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.PartitionKeys, typeof(long)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.PartitionKeyPercent, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.SSTables, typeof(long)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.SSTablePercent, typeof(decimal)).AllowDBNull = true;

            dtKeySpace.Columns.Add(Columns.FlushPercent, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.CompactionPercent, typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add(Columns.RepairPercent, typeof(decimal)).AllowDBNull = true;
            
            dtKeySpace.Columns.Add("DDL", typeof(string));

            dtKeySpace.PrimaryKey = new System.Data.DataColumn[] { dtKeySpace.Columns[Columns.Name], dtKeySpace.Columns[ColumnNames.DataCenter] };

            dtKeySpace.DefaultView.ApplyDefaultSort = false;
            dtKeySpace.DefaultView.AllowDelete = false;
            dtKeySpace.DefaultView.AllowEdit = false;
            dtKeySpace.DefaultView.AllowNew = false;
            dtKeySpace.DefaultView.Sort = string.Format("[Name] ASC, [{0}] ASC", ColumnNames.DataCenter);

            return dtKeySpace;
        }

        struct KSEvtCnts
        {
            public long Flushes;
            public long Compactions;
            public long Repairs;
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

                Logger.Instance.InfoFormat("Loading Keyspace Information for Cluster \"{0}\"", this.Cluster.Name);
                
                var logEvtsCollection = (from logEvt in this.Cluster.GetAllEventsReadOnly(DSEDiagnosticLibrary.LogCassandraEvent.ElementCreationTypes.DCNodeKSDDLTypeClassOnly)
                                         where logEvt.Keyspace != null
                                                    && (logEvt.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Compaction)
                                                            || logEvt.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Flush)
                                                            || logEvt.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Repair))
                                         group logEvt by logEvt.Keyspace into ksGrp                                         
                                         select new
                                         {
                                             KS = ksGrp.Key,
                                             Cnts = new KSEvtCnts()
                                             {
                                                 Flushes = ksGrp.LongCount(i => i.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Flush)),
                                                 Compactions = ksGrp.LongCount(i => i.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Compaction)),
                                                 Repairs = ksGrp.LongCount(e => e.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Repair))
                                             }                                                                                         
                                         }
                                         ).ToArray();


                var clusterTotalStorage = this.Cluster.Nodes
                                                    .Where(n => !n.DSE.StorageUsed.NaN)
                                                    .Select(n => n.DSE.StorageUsed.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB))
                                                    .DefaultIfEmpty()
                                                    .Sum();
                var clusterTotalReads = this.Cluster.Nodes.Select(n => n.DSE.ReadCount).DefaultIfEmpty().Sum();
                var clusterTotalWrite = this.Cluster.Nodes.Select(n => n.DSE.WriteCount).DefaultIfEmpty().Sum();
                var clusterTotalSSTables = this.Cluster.Nodes.Select(n => n.DSE.SSTableCount).DefaultIfEmpty().Sum();
                var clusterTotalKeys = this.Cluster.Nodes.Select(n => n.DSE.KeyCount).DefaultIfEmpty().Sum();
                long clusterTotalFlush = 0;
                long clusterTotalCompaction = 0;
                long clusterTotalRepair = 0;

                if (logEvtsCollection.HasAtLeastOneElement())
                {
                    clusterTotalFlush = logEvtsCollection.Sum(i => i.Cnts.Flushes);
                    clusterTotalCompaction = logEvtsCollection.Sum(i => i.Cnts.Compactions);
                    clusterTotalRepair = logEvtsCollection.Sum(i => i.Cnts.Repairs);
                }

                foreach (var keySpace in this.Cluster.Keyspaces)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    if (!this.WhitelistKeySpaces.Any(n => n == keySpace.Name) && this.IgnoreKeySpaces.Any(n => n == keySpace.Name))
                    {
                        continue;
                    }

                    if (keySpace.LocalStrategy || keySpace.EverywhereStrategy)
                    {                       
                        if(keySpace.IsPreLoaded
                            && (keySpace.IsDSEKeyspace || keySpace.IsSystemKeyspace))                           
                        {
                            var hasActiveMember = keySpace.HasActiveMember;

                            if(!hasActiveMember.HasValue || !hasActiveMember.Value)
                                continue;
                        }

                        if (!this.Table.Rows.Contains(new object[] { keySpace.Name, keySpace.DataCenter?.Name ?? "<NA>" }))
                        {
                            dataRow = this.Table.NewRow();
                            if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);
                            dataRow[Columns.Name] = keySpace.Name;
                            dataRow[Columns.ReplicationStrategy] = keySpace.ReplicationStrategy;
                            dataRow[ColumnNames.DataCenter] = keySpace.DataCenter?.Name ?? "<NA>";
                            dataRow[Columns.ReplicationFactor] = 0;
                            dataRow.SetFieldStringLimit("DDL", keySpace.DDL);
                            this.SetKeyspaceStats(keySpace, dataRow,
                                                        logEvtsCollection.FirstOrDefault(i => i.KS == keySpace)?.Cnts,
                                                        clusterTotalStorage,
                                                        clusterTotalReads,
                                                        clusterTotalWrite,
                                                        clusterTotalKeys,
                                                        clusterTotalSSTables,
                                                        clusterTotalFlush,
                                                        clusterTotalCompaction,
                                                        clusterTotalRepair);
                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;
                        }
                    }
                    else
                    {
                        bool firstRepl = true;
                        
                        foreach (var replication in keySpace.Replications)
                        {
                            if (this.Table.Rows.Contains(new object[] { keySpace.Name, replication.DataCenter.Name }))
                            {
                                continue;
                            }
                            
                            dataRow = this.Table.NewRow();
                            if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);
                            dataRow[Columns.Name] = keySpace.Name;
                            dataRow[Columns.ReplicationStrategy] = keySpace.ReplicationStrategy;
                            dataRow[ColumnNames.DataCenter] = replication.DataCenter.Name;
                            dataRow[Columns.ReplicationFactor] = (int)replication.RF;
                            
                            if (firstRepl)
                            {                                
                                firstRepl = false;
                                this.SetKeyspaceStats(keySpace, dataRow,
                                                        logEvtsCollection.FirstOrDefault(i => i.KS == keySpace)?.Cnts,
                                                        clusterTotalStorage,
                                                        clusterTotalReads,
                                                        clusterTotalWrite,
                                                        clusterTotalKeys,
                                                        clusterTotalSSTables,
                                                        clusterTotalFlush,
                                                        clusterTotalCompaction,
                                                        clusterTotalRepair);
                            }

                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;
                        }
                    }
                }

                Logger.Instance.InfoFormat("Loaded Keyspace Information for Cluster \"{0}\", Total Nbr Items {1:###,###,##0}", this.Cluster.Name, nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Keyspace Information Canceled");
            }
            finally
            {
                this.Table.AcceptChanges();
                this.Table.EndLoadData();
            }

            return this.Table;
        }

        private void SetKeyspaceStats(DSEDiagnosticLibrary.IKeyspace keySpace, DataRow dataRow,
                                        KSEvtCnts? ksCnts,
                                        decimal clusterStorage,
                                        long clusterReads,
                                        long clusterWrites,
                                        long clusterKeys,
                                        long clusterSSTables,
                                        long clusterFlushes,
                                        long clusterCompactions,
                                        long clusterRepairs)
        {
            dataRow[Columns.Tables] = (int)keySpace.Stats.Tables;
            dataRow[Columns.Views] = (int)keySpace.Stats.MaterialViews;
            dataRow[Columns.ColumnTotal] = (int)keySpace.Stats.Columns;
            dataRow[Columns.SecondaryIndexes] = (int)keySpace.Stats.SecondaryIndexes;
            dataRow[Columns.SolrIndexes] = (int)keySpace.Stats.SolrIndexes;
            dataRow[Columns.SASIndexes] = (int)keySpace.Stats.SasIIIndexes;
            dataRow[Columns.CustomIndexes] = (int)keySpace.Stats.CustomIndexes;
            dataRow[Columns.Triggers] = (int)keySpace.Stats.Triggers;
            dataRow[Columns.Functions] = (int)keySpace.Stats.Functions;
            dataRow[Columns.USTs] = (int)keySpace.Stats.UserDefinedTypes;
            dataRow[Columns.Total] = (int)(keySpace.Stats.Tables
                                        + keySpace.Stats.MaterialViews
                                        + keySpace.Stats.SecondaryIndexes
                                        + keySpace.Stats.SolrIndexes
                                        + keySpace.Stats.SasIIIndexes
                                        + keySpace.Stats.CustomIndexes
                                        + keySpace.Stats.Functions
                                        + keySpace.Stats.Triggers
                                        + keySpace.Stats.UserDefinedTypes);

            dataRow[Columns.STCS] = (int)keySpace.Stats.STCS;
            dataRow[Columns.LCS] = (int)keySpace.Stats.LCS;
            dataRow[Columns.DTCS] = (int)keySpace.Stats.DTCS;
            dataRow[Columns.TCS] = (int)keySpace.Stats.TCS;
            dataRow[Columns.TWCS] = (int)keySpace.Stats.TWCS;
            dataRow[Columns.OtherStrategies] = (int)keySpace.Stats.OtherStrategies;

            dataRow[Columns.Active] = (int)keySpace.Stats.NbrActive;
            {
                var totActive = (decimal) (keySpace.Stats.Tables
                                            + keySpace.Stats.MaterialViews
                                            + keySpace.Stats.SecondaryIndexes                                       
                                            + keySpace.Stats.SasIIIndexes
                                            + keySpace.Stats.CustomIndexes);
                if(totActive > 0m)
                {
                    dataRow[Columns.ActivePercent] = ((decimal)keySpace.Stats.NbrActive / totActive);
                }
            }
            
            dataRow.SetFieldStringLimit(Columns.DDL, keySpace.DDL);

            dataRow[Columns.ColumnCollections] = (int)keySpace.Stats.ColumnStat.Collections;
            dataRow[Columns.ColumnBlobs] = (int)keySpace.Stats.ColumnStat.Blobs;
            dataRow[Columns.ColumnStatic] = (int)keySpace.Stats.ColumnStat.Static;
            dataRow[Columns.ColumnFrozen] = (int)keySpace.Stats.ColumnStat.Frozen;
            dataRow[Columns.ColumnTuple] = (int)keySpace.Stats.ColumnStat.Tuple;
            dataRow[Columns.ColumnUDT] = (int)keySpace.Stats.ColumnStat.UDT;
            if(keySpace.Stats.MaxReadRepairChance >= 0)
                dataRow[Columns.ReadRepairChance] = keySpace.Stats.MaxReadRepairChance;
            if(keySpace.Stats.MaxReadRepairDCChance >= 0)
                dataRow[Columns.ReadRepairDCChance] = keySpace.Stats.MaxReadRepairDCChance;
            //dataRow[Columns.ReadRepairPolicy] = (int)keySpace.Stats.
            if (keySpace.Stats.MaxGCGrace > TimeSpan.MinValue)
                dataRow[Columns.GCGrace] = keySpace.Stats.MaxGCGrace;
            if(keySpace.Stats.MaxTTL > TimeSpan.MinValue)
                dataRow[Columns.TTL] = keySpace.Stats.MaxTTL;

            dataRow[Columns.OrderBy] = keySpace.Stats.NbrOrderBys;
            dataRow[Columns.CompactStorage] = keySpace.Stats.NbrCompactStorage;
            
            if (!keySpace.Stats.Storage.NaN)
            {
                var ksStorage = keySpace.Stats.Storage.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);

                dataRow.SetField(Columns.Storage, ksStorage);

                if (clusterStorage > 0m)
                    dataRow.SetField(Columns.StorageUtilized, ksStorage/clusterStorage);
            }
            if (clusterReads > 0)
                dataRow.SetField(Columns.ReadPercent, ((decimal) keySpace.Stats.ReadCount) /  (decimal) clusterReads);
            if (clusterWrites > 0)
                dataRow.SetField(Columns.WritePercent, ((decimal)keySpace.Stats.WriteCount) / (decimal)clusterWrites);
            if (clusterKeys > 0)
                dataRow.SetField(Columns.PartitionKeyPercent, ((decimal)keySpace.Stats.KeyCount) / (decimal)clusterKeys);
            dataRow.SetField(Columns.PartitionKeys, keySpace.Stats.KeyCount);
            if (clusterSSTables > 0)
                dataRow.SetField(Columns.SSTablePercent, ((decimal)keySpace.Stats.SSTableCount) / (decimal)clusterSSTables);
            dataRow.SetField(Columns.SSTables, keySpace.Stats.SSTableCount);

            if(ksCnts.HasValue)
            {
                if(ksCnts.Value.Flushes > 0)
                    dataRow.SetField(Columns.FlushPercent, (decimal) ksCnts.Value.Flushes / (decimal) clusterFlushes);
                if (ksCnts.Value.Compactions > 0)
                    dataRow.SetField(Columns.CompactionPercent, (decimal)ksCnts.Value.Compactions / (decimal)clusterCompactions);
                if (ksCnts.Value.Repairs > 0)
                    dataRow.SetField(Columns.RepairPercent, (decimal)ksCnts.Value.Repairs / (decimal)clusterRepairs);
            }

            if(keySpace.Stats.NbrNodeSync > 0)
                dataRow.SetField(Columns.TblsNodeSync, (int) keySpace.Stats.NbrNodeSync);
        }

    }
}
