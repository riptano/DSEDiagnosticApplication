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
        public KeyspaceDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtKeySpace = new DataTable(TableNames.Keyspaces, TableNames.Namespace);

            if (this.SessionId.HasValue) dtKeySpace.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtKeySpace.Columns.Add("Name", typeof(string));
            dtKeySpace.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtKeySpace.Columns.Add("Replication Strategy", typeof(string));            
            dtKeySpace.Columns.Add("Replication Factor", typeof(int));

            dtKeySpace.Columns.Add("Tables", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Views", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Secondary Indexes", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("solr Indexes", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("SAS Indexes", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Custom Indexes", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Functions", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Triggers", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Total", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Active", typeof(int)).AllowDBNull = true;

            dtKeySpace.Columns.Add("STCS", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("LCS", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("DTCS", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("TCS", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("TWCS", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Other Strategies", typeof(int)).AllowDBNull = true;

            dtKeySpace.Columns.Add("Column Total", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Column Collections", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Column Blobs", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Column Static", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Column Frozen", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Column Tuple", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Column UDT", typeof(int)).AllowDBNull = true;

            dtKeySpace.Columns.Add("Read-Repair Chance (Max)", typeof(decimal)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Read-Repair DC Chance", typeof(decimal)).AllowDBNull = true;
            //dtKeySpace.Columns.Add("Read-Repair Policy (Majority)", typeof(string)).AllowDBNull = true;

            dtKeySpace.Columns.Add("GC Grace (Max)", typeof(TimeSpan)).AllowDBNull = true;
            dtKeySpace.Columns.Add("TTL (Max)", typeof(TimeSpan)).AllowDBNull = true;

            dtKeySpace.Columns.Add("OrderBy", typeof(int)).AllowDBNull = true;

            dtKeySpace.Columns.Add("Storage (MB)", typeof(decimal)).AllowDBNull = true;

            dtKeySpace.Columns.Add("DDL", typeof(string));

            dtKeySpace.PrimaryKey = new System.Data.DataColumn[] { dtKeySpace.Columns["Name"], dtKeySpace.Columns[ColumnNames.DataCenter] };

            dtKeySpace.DefaultView.ApplyDefaultSort = false;
            dtKeySpace.DefaultView.AllowDelete = false;
            dtKeySpace.DefaultView.AllowEdit = false;
            dtKeySpace.DefaultView.AllowNew = false;
            dtKeySpace.DefaultView.Sort = string.Format("[Name] ASC, [{0}] ASC", ColumnNames.DataCenter);

            return dtKeySpace;
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

                foreach (var keySpace in this.Cluster.Keyspaces)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    if (this.IgnoreKeySpaces.Any(n => n == keySpace.Name))
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
                            dataRow["Name"] = keySpace.Name;
                            dataRow["Replication Strategy"] = keySpace.ReplicationStrategy;
                            dataRow[ColumnNames.DataCenter] = keySpace.DataCenter?.Name ?? "<NA>";
                            dataRow["Replication Factor"] = 0;
                            dataRow.SetFieldStringLimit("DDL", keySpace.DDL);
                            this.SetKeyspaceStats(keySpace, dataRow);
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
                            dataRow["Name"] = keySpace.Name;
                            dataRow["Replication Strategy"] = keySpace.ReplicationStrategy;
                            dataRow["Data Center"] = replication.DataCenter.Name;
                            dataRow["Replication Factor"] = (int)replication.RF;
                            
                            if (firstRepl)
                            {
                                firstRepl = false;
                                this.SetKeyspaceStats(keySpace, dataRow);
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

        private void SetKeyspaceStats(DSEDiagnosticLibrary.IKeyspace keySpace, DataRow dataRow)
        {
            dataRow["Tables"] = (int)keySpace.Stats.Tables;
            dataRow["Views"] = (int)keySpace.Stats.MaterialViews;
            dataRow["Column Total"] = (int)keySpace.Stats.Columns;
            dataRow["Secondary Indexes"] = (int)keySpace.Stats.SecondaryIndexes;
            dataRow["solr Indexes"] = (int)keySpace.Stats.SolrIndexes;
            dataRow["SAS Indexes"] = (int)keySpace.Stats.SasIIIndexes;
            dataRow["Custom Indexes"] = (int)keySpace.Stats.CustomIndexes;
            dataRow["Triggers"] = (int)keySpace.Stats.Triggers;
            dataRow["Functions"] = (int)keySpace.Stats.Functions;
            dataRow["Total"] = (int)(keySpace.Stats.Tables
                                        + keySpace.Stats.MaterialViews
                                        + keySpace.Stats.SecondaryIndexes
                                        + keySpace.Stats.SolrIndexes
                                        + keySpace.Stats.SasIIIndexes
                                        + keySpace.Stats.CustomIndexes
                                        + keySpace.Stats.Functions
                                        + keySpace.Stats.Triggers);

            dataRow["STCS"] = (int)keySpace.Stats.STCS;
            dataRow["LCS"] = (int)keySpace.Stats.LCS;
            dataRow["DTCS"] = (int)keySpace.Stats.DTCS;
            dataRow["TCS"] = (int)keySpace.Stats.TCS;
            dataRow["TWCS"] = (int)keySpace.Stats.TWCS;
            dataRow["Other Strategies"] = (int)keySpace.Stats.OtherStrategies;

            dataRow["Active"] = (int)keySpace.Stats.NbrActive;
            dataRow.SetFieldStringLimit("DDL", keySpace.DDL);

            dataRow["Column Collections"] = (int)keySpace.Stats.ColumnStat.Collections;
            dataRow["Column Blobs"] = (int)keySpace.Stats.ColumnStat.Blobs;
            dataRow["Column Static"] = (int)keySpace.Stats.ColumnStat.Static;
            dataRow["Column Frozen"] = (int)keySpace.Stats.ColumnStat.Frozen;
            dataRow["Column Tuple"] = (int)keySpace.Stats.ColumnStat.Tuple;
            dataRow["Column UDT"] = (int)keySpace.Stats.ColumnStat.UDT;
            if(keySpace.Stats.MaxReadRepairChance >= 0)
                dataRow["Read-Repair Chance (Max)"] = keySpace.Stats.MaxReadRepairChance;
            if(keySpace.Stats.MaxReadRepairDCChance >= 0)
                dataRow["Read-Repair DC Chance"] = keySpace.Stats.MaxReadRepairDCChance;
            //dataRow["Read-Repair Policy (Majority)"] = (int)keySpace.Stats.
            if(keySpace.Stats.MaxGCGrace > TimeSpan.MinValue)
                dataRow["GC Grace (Max)"] = keySpace.Stats.MaxGCGrace;
            if(keySpace.Stats.MaxTTL > TimeSpan.MinValue)
                dataRow["TTL (Max)"] = keySpace.Stats.MaxTTL;

            dataRow["OrderBy"] = keySpace.Stats.NbrOrderBys;

            dataRow.SetFieldToDecimal("Storage (MB)", keySpace.StorageUtilized, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
        }

    }
}
