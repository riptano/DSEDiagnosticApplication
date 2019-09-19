using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class CQLDDLDataTable : DataTableLoad
    {
        public CQLDDLDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtDDL = new DataTable(TableNames.CQLDLL, TableNames.Namespace);
            dtDDL.CaseSensitive();

            if(this.SessionId.HasValue) dtDDL.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtDDL.Columns.Add("Active", typeof(bool));//a
            dtDDL.Columns.Add(ColumnNames.KeySpace, typeof(string));//b
            dtDDL.Columns.Add("Name", typeof(string));
            dtDDL.Columns.Add("Type", typeof(string)).AllowDBNull = false; //d
            dtDDL.Columns.Add("Partition Key", typeof(string)).AllowDBNull = true;
            dtDDL.Columns.Add("Cluster Key", typeof(string)).AllowDBNull = true;
            dtDDL.Columns.Add("Compaction Strategy", typeof(string)).AllowDBNull = true;
            dtDDL.Columns.Add("Compression", typeof(string)).AllowDBNull = true; //h
            dtDDL.Columns.Add("Chance", typeof(decimal)).AllowDBNull = true;//i
            dtDDL.Columns.Add("DC Chance", typeof(decimal)).AllowDBNull = true;//j
            dtDDL.Columns.Add("Policy", typeof(string)).AllowDBNull = true;//k
            dtDDL.Columns.Add("GC Grace Period", typeof(TimeSpan)).AllowDBNull = true;//l
            dtDDL.Columns.Add("TTL", typeof(TimeSpan)).AllowDBNull = true; //m
            dtDDL.Columns.Add("Memtable Flush Period", typeof(long)).AllowDBNull = true;
            dtDDL.Columns.Add("Collections", typeof(int)).AllowDBNull = true;//n
            dtDDL.Columns.Add("Counters", typeof(int)).AllowDBNull = true;//o
            dtDDL.Columns.Add("Blobs", typeof(int)).AllowDBNull = true;//p
            dtDDL.Columns.Add("Static", typeof(int)).AllowDBNull = true;//q
            dtDDL.Columns.Add("Frozen", typeof(int)).AllowDBNull = true;//r
            dtDDL.Columns.Add("Tuple", typeof(int)).AllowDBNull = true;//s
            dtDDL.Columns.Add("UDT", typeof(int)).AllowDBNull = true;            
            dtDDL.Columns.Add("Total", typeof(int)).AllowDBNull = true;
            dtDDL.Columns.Add("HasOrderBy", typeof(bool)).AllowDBNull = true;
            dtDDL.Columns.Add("Compact Storage", typeof(bool)).AllowDBNull = true;
            dtDDL.Columns.Add("Node Sync", typeof(bool)).AllowDBNull = true;
            dtDDL.Columns.Add("Associated Table", typeof(string)).AllowDBNull = true;
            dtDDL.Columns.Add("Index", typeof(bool)).AllowDBNull = true; 
            dtDDL.Columns.Add("NbrIndexes", typeof(int)).AllowDBNull = true;
            dtDDL.Columns.Add("NbrMVs", typeof(int)).AllowDBNull = true;
            dtDDL.Columns.Add("NbrTriggers", typeof(int)).AllowDBNull = true;            
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.Storage, typeof(decimal)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.StorageUtilized, typeof(decimal)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.ReadPercent, typeof(decimal)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.WritePercent, typeof(decimal)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.PartitionKeys, typeof(long)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.PartitionKeyPercent, typeof(decimal)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.SSTables, typeof(long)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.SSTablePercent, typeof(decimal)).AllowDBNull = true;

            dtDDL.Columns.Add(KeyspaceDataTable.Columns.FlushPercent, typeof(decimal)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.CompactionPercent, typeof(decimal)).AllowDBNull = true;
            dtDDL.Columns.Add(KeyspaceDataTable.Columns.RepairPercent, typeof(decimal)).AllowDBNull = true;

            dtDDL.Columns.Add(KeyspaceDataTable.Columns.DDL, typeof(string));

            dtDDL.PrimaryKey = new System.Data.DataColumn[] { dtDDL.Columns[ColumnNames.KeySpace], dtDDL.Columns["Name"] };

            dtDDL.DefaultView.ApplyDefaultSort = false;
            dtDDL.DefaultView.AllowDelete = false;
            dtDDL.DefaultView.AllowEdit = false;
            dtDDL.DefaultView.AllowNew = false;
            dtDDL.DefaultView.Sort = string.Format("[{0}] ASC, [Name] ASC", ColumnNames.KeySpace);

            return dtDDL;
        }

        struct ObjEvtCnts
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

                Logger.Instance.InfoFormat("Loading CQL DDL Information for Cluster \"{0}\"", this.Cluster.Name);

                var logEvtsCollection = (from logEvt in this.Cluster.GetAllEventsReadOnly()                                         
                                         where logEvt.TableViewIndex != null
                                                    && (logEvt.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Compaction)
                                                            || logEvt.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Flush)
                                                            || logEvt.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Repair))
                                         group logEvt by logEvt.TableViewIndex into objGrp
                                         select new
                                         {
                                             ObjIns = objGrp.Key,
                                             Cnts = new ObjEvtCnts()
                                             {
                                                 Flushes = objGrp.LongCount(i => i.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Flush)),
                                                 Compactions = objGrp.LongCount(i => i.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Compaction)),
                                                 Repairs = objGrp.LongCount(e => e.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.Repair))
                                             }
                                         }
                                         ).ToArray();


                var clusterTotalStorage = this.Cluster.Nodes
                                                    .Where(n => !n.DSE.StorageUsed.NaN)
                                                    .Select(n => n.DSE.StorageUsed.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB))
                                                    .DefaultIfEmpty()
                                                    .Sum();
                var clusterTotalReads = this.Cluster.Nodes.Select(n => n.DSE.ReadCount).DefaultIfEmpty().Sum();
                var clusterTotalWrites = this.Cluster.Nodes.Select(n => n.DSE.WriteCount).DefaultIfEmpty().Sum();
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
                    var ddlCnt = 0;

                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading DDL for Keyspace \"{0}\"", keySpace.Name);

                    if (this.IgnoreKeySpaces.Any(n => n == keySpace.Name))
                    {
                        continue;
                    }

                    foreach (var ddlItem in keySpace.DDLs)
                    {
                        bool addStats = false;

                        this.CancellationToken.ThrowIfCancellationRequested();

                        if (this.Table.Rows.Contains(new object[] { keySpace.Name, ddlItem.Name }))
                        {
                            continue;
                        }

                        dataRow = this.Table.NewRow();

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow.SetField("Keyspace Name", keySpace.Name);
                        dataRow.SetField("Name", ddlItem.Name);
                        dataRow.SetField("Type", ddlItem.GetType().Name);
                        dataRow.SetFieldStringLimit(KeyspaceDataTable.Columns.DDL, ddlItem.DDL);
                        dataRow.SetField("Total", ddlItem.Items);

                        if (ddlItem is IDDLStmt && ((IDDLStmt)ddlItem).IsActive.HasValue)
                        {
                            dataRow.SetField("Active", ((IDDLStmt)ddlItem).IsActive.Value);
                        }

                        if (ddlItem is DSEDiagnosticLibrary.ICQLTrigger icqlTrigger)
                        {
                            dataRow.SetField("Associated Table", icqlTrigger.Table.FullName);
                        }
                        else if (ddlItem is DSEDiagnosticLibrary.ICQLIndex icqlIdx)
                        {
                            dataRow.SetField("Associated Table", icqlIdx.Table.FullName);
                            dataRow.SetField("Index", true);
                            dataRow.SetFieldStringLimit("Partition Key", string.Join(", ", icqlIdx.Columns
                                                                                    .Select(cf => cf.PrettyPrint())));
                            if (!string.IsNullOrEmpty(icqlIdx.UsingClass))
                            {
                                dataRow.SetField("Compaction Strategy", icqlIdx.UsingClassNormalized);
                            }
                            addStats = true;
                            {
                                var idxStorage = icqlIdx.Storage;

                                if (!idxStorage.NaN)
                                {
                                    var idxSize = idxStorage.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);

                                    dataRow.SetField(KeyspaceDataTable.Columns.Storage, idxSize);

                                    if (clusterTotalStorage > 0m)
                                        dataRow.SetField(KeyspaceDataTable.Columns.StorageUtilized, idxSize / clusterTotalStorage);
                                }
                                if (clusterTotalReads > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.ReadPercent, ((decimal)icqlIdx.ReadCount) / (decimal)clusterTotalReads);                                
                            }                            
                        }
                        else if (ddlItem is DSEDiagnosticLibrary.ICQLUserDefinedType icqlUDT)
                        {
                            dataRow.SetField("Name", ddlItem.Name + " (Type)");
                            dataRow.SetField("Collections", icqlUDT.Columns.Count(c => c.CQLType.HasCollection));
                            dataRow.SetField("Counters", icqlUDT.Columns.Count(c => c.CQLType.IsCounter));
                            dataRow.SetField("Blobs", icqlUDT.Columns.Count(c => c.CQLType.HasBlob));
                            dataRow.SetField("Static", icqlUDT.Columns.Count(c => c.IsStatic));
                            dataRow.SetField("Frozen", icqlUDT.Columns.Count(c => c.CQLType.HasFrozen));
                            dataRow.SetField("Tuple", icqlUDT.Columns.Count(c => c.CQLType.HasTuple));
                            dataRow.SetField("UDT", icqlUDT.Columns.Count(c => c.CQLType.HasUDT));
                        }
                        else if (ddlItem is DSEDiagnosticLibrary.ICQLTable icqlTbl)
                        {
                            dataRow.SetField("Total", (int)icqlTbl.Stats.NbrColumns);
                            dataRow.SetField("Collections", (int)icqlTbl.Stats.Collections);
                            dataRow.SetField("Counters", (int)icqlTbl.Stats.Counters);
                            dataRow.SetField("Blobs", (int)icqlTbl.Stats.Blobs);
                            dataRow.SetField("Static", (int)icqlTbl.Stats.Statics);
                            dataRow.SetField("Frozen", (int)icqlTbl.Stats.Frozens);
                            dataRow.SetField("Tuple", (int)icqlTbl.Stats.Tuples);
                            dataRow.SetField("UDT", (int)icqlTbl.Stats.UDTs);

                            if(icqlTbl.OrderByCols.HasAtLeastOneElement())
                                dataRow.SetField("HasOrderBy", true);

                            {
                                var compactStorage = icqlTbl.GetPropertyValue("compact storage");

                                if(compactStorage != null && (bool)compactStorage)
                                    dataRow.SetField("Compact Storage", true);
                            }


                            dataRow.SetFieldStringLimit("Partition Key", string.Join(",", icqlTbl.PrimaryKeys.Select(k => k.Name + ' ' + k.CQLType.Name)));
                            dataRow.SetFieldStringLimit("Cluster Key", string.Join(",", icqlTbl.ClusteringKeys.Select(k => k.Name + ' ' + k.CQLType.Name)));
                            dataRow.SetField("Compaction Strategy", icqlTbl.Compaction);
                            dataRow.SetField("Compression", icqlTbl.Compression);
                            dataRow.SetField("Chance", icqlTbl.GetPropertyValue("read_repair_chance"));
                            dataRow.SetField("DC Chance", icqlTbl.GetPropertyValue("dclocal_read_repair_chance"));
                            dataRow.SetField("Policy", icqlTbl.GetPropertyValue("speculative_retry"));
                            dataRow.SetField("GC Grace Period", icqlTbl.GetPropertyValue("gc_grace_seconds"));
                            dataRow.SetField("TTL", icqlTbl.GetPropertyValue("default_time_to_live"));
                            dataRow.SetField("Memtable Flush Period", icqlTbl.GetPropertyValueInMSLong("memtable_flush_period_in_ms"));
                            addStats = true;

                            {
                                var stats = icqlTbl.Stats;

                                if (!stats.Storage.NaN)
                                {
                                    var tblStorage = stats.Storage.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);

                                    dataRow.SetField(KeyspaceDataTable.Columns.Storage, tblStorage);

                                    if (clusterTotalStorage > 0m)
                                        dataRow.SetField(KeyspaceDataTable.Columns.StorageUtilized, tblStorage / clusterTotalStorage);
                                }
                                if (clusterTotalReads > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.ReadPercent, ((decimal)stats.ReadCount) / (decimal)clusterTotalReads);
                                if (clusterTotalWrites > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.WritePercent, ((decimal)stats.WriteCount) / (decimal)clusterTotalWrites);
                                if (clusterTotalKeys > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.PartitionKeyPercent, ((decimal)stats.KeyCount) / (decimal)clusterTotalKeys);
                                dataRow.SetField(KeyspaceDataTable.Columns.PartitionKeys, stats.KeyCount);
                                if (clusterTotalSSTables > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.SSTablePercent, ((decimal)stats.SSTableCount) / (decimal)clusterTotalSSTables);
                                dataRow.SetField(KeyspaceDataTable.Columns.SSTables, stats.SSTableCount);
                            }
                            {
                                var nbrIndexes = (int)icqlTbl.Stats.NbrCustomIndexes
                                                                    + icqlTbl.Stats.NbrSasIIIndexes
                                                                    + icqlTbl.Stats.NbrSecondaryIndexes
                                                                    + icqlTbl.Stats.NbrSolrIndexes;
                                if(nbrIndexes > 0)
                                    dataRow.SetField("NbrIndexes", nbrIndexes);
                            }
                            
                            if(icqlTbl.Stats.NbrMaterializedViews > 0)
                                dataRow.SetField("NbrMVs", icqlTbl.Stats.NbrMaterializedViews);
                            if (icqlTbl.Stats.NbrTriggers > 0)
                                dataRow.SetField("NbrTriggers", icqlTbl.Stats.NbrTriggers);
                            if(icqlTbl.NodeSyncEnabled)
                                dataRow.SetField("Node Sync", true);

                            if (ddlItem is DSEDiagnosticLibrary.ICQLMaterializedView icqlMV)
                            {
                                dataRow.SetField("Associated Table", icqlMV.Table.FullName);
                            }
                        }

                        if(addStats)
                        {
                            var objCnts = logEvtsCollection.FirstOrDefault(i => i.ObjIns == ddlItem)?.Cnts;

                            if (objCnts.HasValue)
                            {
                                if (objCnts.Value.Flushes > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.FlushPercent, (decimal)objCnts.Value.Flushes / (decimal)clusterTotalFlush);
                                if (objCnts.Value.Compactions > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.CompactionPercent, (decimal)objCnts.Value.Compactions / (decimal)clusterTotalCompaction);
                                if (objCnts.Value.Repairs > 0)
                                    dataRow.SetField(KeyspaceDataTable.Columns.RepairPercent, (decimal)objCnts.Value.Repairs / (decimal)clusterTotalRepair);
                            }
                        }

                        this.Table.Rows.Add(dataRow);
                        ++ddlCnt;
                        ++nbrItems;
                    }

                    Logger.Instance.InfoFormat("Loaded DDL for Keyspace \"{0}\", Total Nbr Items {1:###,###,##0}", this.Cluster.Name, ddlCnt);
                }

                Logger.Instance.InfoFormat("Loaded CQL DDL Information for Cluster \"{0}\", Total Nbr Items {1:###,###,##0}", this.Cluster.Name, nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading CQL DDL Information Canceled");
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
