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
        public KeyspaceDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null)
            : base(cluster, cancellationSource)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtKeySpace = new DataTable(TableNames.Keyspaces, TableNames.Namespace);

            dtKeySpace.Columns.Add("Name", typeof(string));//a
            dtKeySpace.Columns.Add("Replication Strategy", typeof(string));
            dtKeySpace.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtKeySpace.Columns.Add("Replication Factor", typeof(int));//d
            dtKeySpace.Columns.Add("Tables", typeof(int)).AllowDBNull = true;//e
            dtKeySpace.Columns.Add("Views", typeof(int)).AllowDBNull = true;//f
            dtKeySpace.Columns.Add("Columns", typeof(int)).AllowDBNull = true;//g
            dtKeySpace.Columns.Add("Secondary Indexes", typeof(int)).AllowDBNull = true;//h
            dtKeySpace.Columns.Add("solr Indexes", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("SAS Indexes", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Custom Indexes", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Triggers", typeof(int)).AllowDBNull = true;
            dtKeySpace.Columns.Add("Total", typeof(int)).AllowDBNull = true;//m
            dtKeySpace.Columns.Add("Active", typeof(int)).AllowDBNull = true;//n
            dtKeySpace.Columns.Add("STCS", typeof(int)).AllowDBNull = true;//o
            dtKeySpace.Columns.Add("LCS", typeof(int)).AllowDBNull = true;//p
            dtKeySpace.Columns.Add("DTCS", typeof(int)).AllowDBNull = true;//q
            dtKeySpace.Columns.Add("TCS", typeof(int)).AllowDBNull = true;//r
            dtKeySpace.Columns.Add("TWCS", typeof(int)).AllowDBNull = true;//s
            dtKeySpace.Columns.Add("Other Strategies", typeof(int)).AllowDBNull = true;//t
            dtKeySpace.Columns.Add("DDL", typeof(string));//u

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
                        if (!this.Table.Rows.Contains(new object[] { keySpace.Name, keySpace.DataCenter.Name }))
                        {
                            dataRow = this.Table.NewRow();
                            dataRow["Name"] = keySpace.Name;
                            dataRow["Replication Strategy"] = keySpace.ReplicationStrategy;
                            dataRow[ColumnNames.DataCenter] = keySpace.DataCenter.Name;
                            dataRow["Replication Factor"] = 0;
                            dataRow["DDL"] = keySpace.DDL;
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
                            dataRow["Name"] = keySpace.Name;
                            dataRow["Replication Strategy"] = keySpace.ReplicationStrategy;
                            dataRow["Data Center"] = replication.DataCenter.Name;
                            dataRow["Replication Factor"] = (int)replication.RF;
                            dataRow["DDL"] = keySpace.DDL;

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
            dataRow["Columns"] = (int)keySpace.Stats.Columns;
            dataRow["Secondary Indexes"] = (int)keySpace.Stats.SecondaryIndexes;
            dataRow["solr Indexes"] = (int)keySpace.Stats.SolrIndexes;
            dataRow["SAS Indexes"] = (int)keySpace.Stats.SasIIIndexes;
            dataRow["Custom Indexes"] = (int)keySpace.Stats.CustomIndexes;
            dataRow["Triggers"] = (int)keySpace.Stats.Triggers;
            dataRow["Total"] = (int)(keySpace.Stats.Tables
                                        + keySpace.Stats.MaterialViews
                                        + keySpace.Stats.SecondaryIndexes
                                        + keySpace.Stats.SolrIndexes
                                        + keySpace.Stats.SasIIIndexes
                                        + keySpace.Stats.CustomIndexes);
            dataRow["STCS"] = (int)keySpace.Stats.STCS;
            dataRow["LCS"] = (int)keySpace.Stats.LCS;
            dataRow["DTCS"] = (int)keySpace.Stats.DTCS;
            dataRow["TCS"] = (int)keySpace.Stats.TCS;
            dataRow["TWCS"] = (int)keySpace.Stats.TWCS;
            dataRow["Other Strategies"] = (int)keySpace.Stats.OtherStrategies;
            dataRow["Active"] = (int)keySpace.Stats.NbrActive;
        }

    }
}
