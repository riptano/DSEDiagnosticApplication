using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
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

        public override DataTable CreateInitializationTable()
        {
            var dtPartitionInfo = new DataTable(TableNames.CommonPartitionKey, TableNames.Namespace);

            if (this.SessionId.HasValue) dtPartitionInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtPartitionInfo.Columns.Add("Partition Key", typeof(string));
            dtPartitionInfo.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtPartitionInfo.Columns.Add(ColumnNames.KeySpace, typeof(string));
            dtPartitionInfo.Columns.Add(ColumnNames.Table, typeof(string));
            dtPartitionInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtPartitionInfo.Columns.Add("Local read count", typeof(long)); //f
            dtPartitionInfo.Columns.Add("Local write count", typeof(long)); //g
            dtPartitionInfo.Columns.Add("Local Total count", typeof(long)); //h
            dtPartitionInfo.Columns.Add("Partition Keys count", typeof(long)); //i

            dtPartitionInfo.DefaultView.ApplyDefaultSort = false;
            dtPartitionInfo.DefaultView.AllowDelete = false;
            dtPartitionInfo.DefaultView.AllowEdit = false;
            dtPartitionInfo.DefaultView.AllowNew = false;
            dtPartitionInfo.DefaultView.Sort = string.Format("[Partition Key] ASC, [{0}] ASC, [{1}] ASC, [{2}] ASC, [{3}] ASC",
                                                                ColumnNames.DataCenter,
                                                                ColumnNames.KeySpace,
                                                                ColumnNames.Table,
                                                                ColumnNames.NodeIPAddress);

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
                                            Node = s.Node,
                                            Table = s.TableViewIndex,
                                            LocalReads = s.Data.GetPropertyValue(Properties.Settings.Default.CFStatsLocalReadCountName) ?? 0L,
                                            LocalWrites = s.Data.GetPropertyValue(Properties.Settings.Default.CFStatsLocalWriteCountName) ?? 0L,
                                            PartitionKeys = s.Data.GetPropertyValue(Properties.Settings.Default.CFStatsNbrKeys) 
                                                                ?? s.Data.GetPropertyValue(Properties.Settings.Default.CFStatsNbrKeys1)
                                                                ?? 0L
                                        });

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

                            dataRow = this.Table.NewRow();

                            if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                            dataRow.SetFieldStringLimit("Partition Key", string.Join(",", cpkStat.PartitionKey.Select(k => k.Name + ' ' + k.CQLType.Name)));

                            dataRow.SetField(ColumnNames.DataCenter, nodeInfo.DC.Name);
                            dataRow.SetField(ColumnNames.KeySpace, cpkTable.Keyspace.Name);
                            dataRow.SetField(ColumnNames.Table, cpkTable.Name);
                            dataRow.SetField(ColumnNames.NodeIPAddress, nodeInfo.Node.Id.NodeName());
                            dataRow.SetField("Local read count", nodeInfo.LocalReads);
                            dataRow.SetField("Local write count", nodeInfo.LocalWrites);
                            dataRow.SetField("Local Total count", ((long) (dynamic) nodeInfo.LocalWrites) + ((long) (dynamic) nodeInfo.LocalReads));
                            dataRow.SetField("Partition Keys count", nodeInfo.PartitionKeys);

                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;
                        }
                    }                    
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
