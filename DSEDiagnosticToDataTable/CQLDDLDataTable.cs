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
            dtDDL.Columns.Add("Collections", typeof(int)).AllowDBNull = true;//n
            dtDDL.Columns.Add("Counters", typeof(int)).AllowDBNull = true;//o
            dtDDL.Columns.Add("Blobs", typeof(int)).AllowDBNull = true;//p
            dtDDL.Columns.Add("Static", typeof(int)).AllowDBNull = true;//q
            dtDDL.Columns.Add("Frozen", typeof(int)).AllowDBNull = true;//r
            dtDDL.Columns.Add("Tuple", typeof(int)).AllowDBNull = true;//s
            dtDDL.Columns.Add("UDT", typeof(int)).AllowDBNull = true;//t
            dtDDL.Columns.Add("Total", typeof(int)).AllowDBNull = true;//u
            dtDDL.Columns.Add("HasOrderBy", typeof(bool)).AllowDBNull = true;//v
            dtDDL.Columns.Add("Associated Table", typeof(string)).AllowDBNull = true;//w
            dtDDL.Columns.Add("Index", typeof(bool)).AllowDBNull = true; //x
            dtDDL.Columns.Add("DDL", typeof(string));//z

            dtDDL.PrimaryKey = new System.Data.DataColumn[] { dtDDL.Columns[ColumnNames.KeySpace], dtDDL.Columns["Name"] };

            dtDDL.DefaultView.ApplyDefaultSort = false;
            dtDDL.DefaultView.AllowDelete = false;
            dtDDL.DefaultView.AllowEdit = false;
            dtDDL.DefaultView.AllowNew = false;
            dtDDL.DefaultView.Sort = string.Format("[{0}] ASC, [Name] ASC", ColumnNames.KeySpace);

            return dtDDL;
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
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if (this.Table.Rows.Contains(new object[] { keySpace.Name, ddlItem.Name }))
                        {
                            continue;
                        }

                        dataRow = this.Table.NewRow();

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow["Keyspace Name"] = keySpace.Name;
                        dataRow["Name"] = ddlItem.Name;
                        dataRow["Type"] = ddlItem.GetType().Name;
                        dataRow.SetFieldStringLimit("DDL", ddlItem.DDL);
                        dataRow["Total"] = ddlItem.Items;

                        if (ddlItem is IDDLStmt && ((IDDLStmt)ddlItem).IsActive.HasValue)
                        {
                            dataRow["Active"] = ((IDDLStmt)ddlItem).IsActive.Value;
                        }

                        if (ddlItem is DSEDiagnosticLibrary.ICQLTrigger)
                        {
                            dataRow["Associated Table"] = ((DSEDiagnosticLibrary.ICQLTrigger)ddlItem).Table.FullName;
                        }
                        else if (ddlItem is DSEDiagnosticLibrary.ICQLIndex)
                        {
                            dataRow["Associated Table"] = ((DSEDiagnosticLibrary.ICQLIndex)ddlItem).Table.FullName;
                            dataRow["Index"] = true;
                            dataRow.SetFieldStringLimit("Partition Key", string.Join(", ", ((DSEDiagnosticLibrary.ICQLIndex)ddlItem).Columns
                                                                                    .Select(cf => cf.PrettyPrint())));
                            if (!string.IsNullOrEmpty(((DSEDiagnosticLibrary.ICQLIndex)ddlItem).UsingClass))
                            {
                                dataRow["Compaction Strategy"] = ((DSEDiagnosticLibrary.ICQLIndex)ddlItem).UsingClassNormalized;
                            }
                        }
                        else if (ddlItem is DSEDiagnosticLibrary.ICQLUserDefinedType)
                        {
                            dataRow["Name"] = ddlItem.Name + " (Type)";
                            dataRow["Collections"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.HasCollection);
                            dataRow["Counters"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.IsCounter);
                            dataRow["Blobs"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.HasBlob);
                            dataRow["Static"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.IsStatic);
                            dataRow["Frozen"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.HasFrozen);
                            dataRow["Tuple"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.HasTuple);
                            dataRow["UDT"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.HasUDT);
                        }
                        else if (ddlItem is DSEDiagnosticLibrary.ICQLTable)
                        {
                            dataRow["Total"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.NbrColumns;
                            dataRow["Collections"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Collections;
                            dataRow["Counters"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Counters;
                            dataRow["Blobs"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Blobs;
                            dataRow["Static"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Statics;
                            dataRow["Frozen"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Frozens;
                            dataRow["Tuple"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Tuples;
                            dataRow["UDT"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.UDTs;
                            dataRow["HasOrderBy"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).OrderByCols.HasAtLeastOneElement();

                            dataRow.SetFieldStringLimit("Partition Key", string.Join(",", ((DSEDiagnosticLibrary.ICQLTable)ddlItem).PrimaryKeys.Select(k => k.Name + ' ' + k.CQLType.Name)));
                            dataRow.SetFieldStringLimit("Cluster Key", string.Join(",", ((DSEDiagnosticLibrary.ICQLTable)ddlItem).ClusteringKeys.Select(k => k.Name + ' ' + k.CQLType.Name)));
                            dataRow["Compaction Strategy"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Compaction;
                            dataRow["Compression"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Compression;
                            dataRow["Chance"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("read_repair_chance");
                            dataRow["DC Chance"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("dclocal_read_repair_chance");
                            dataRow["Policy"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("speculative_retry");
                            dataRow["GC Grace Period"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("gc_grace_seconds");
                            dataRow["TTL"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("default_time_to_live");

                            if (ddlItem is DSEDiagnosticLibrary.ICQLMaterializedView)
                            {
                                dataRow["Associated Table"] = ((DSEDiagnosticLibrary.ICQLMaterializedView)ddlItem).Table.FullName;
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
