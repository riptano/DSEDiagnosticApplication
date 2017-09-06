using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Common;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class TPStatsDataTable : DataTableLoad
    {
        public TPStatsDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null)
            : base(cluster, cancellationSource)
        {
        }

        public override DataTable CreateInitializationTable()
        {
            var dtTPStats = new DataTable(TableNames.NodeStats, TableNames.Namespace);

            dtTPStats.Columns.Add(ColumnNames.Source, typeof(string)); //A
            dtTPStats.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtTPStats.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtTPStats.Columns.Add("Attribute", typeof(string));

            //Pool Name                    Active   Pending      Completed   Blocked  All time blocked
            //MutationStage                     0         0       28117575         0                 0
            //
            //Attribute Key: "Pool Name.MutationStage.Active"
            //
            //Message type           Dropped
            //READ                         0
            //
            //Attribute Key: "READ.Dropped"

            dtTPStats.Columns.Add("Active", typeof(long)).AllowDBNull = true; //E
            dtTPStats.Columns.Add("Pending", typeof(long)).AllowDBNull = true;
            dtTPStats.Columns.Add("Completed", typeof(long)).AllowDBNull = true;
            dtTPStats.Columns.Add("Blocked", typeof(long)).AllowDBNull = true;//H
            dtTPStats.Columns.Add("All time blocked", typeof(long)).AllowDBNull = true;
            dtTPStats.Columns.Add("Dropped", typeof(long)).AllowDBNull = true;
            dtTPStats.Columns.Add("Latency (ms)", typeof(int)).AllowDBNull = true;
            dtTPStats.Columns.Add("Occurrences", typeof(int)).AllowDBNull = true; //L
            dtTPStats.Columns.Add("Size (mb)", typeof(decimal)).AllowDBNull = true;
            dtTPStats.Columns.Add("GC Eden Space Change (mb)", typeof(decimal)).AllowDBNull = true;
            dtTPStats.Columns.Add("GC Survivor Space Change (mb)", typeof(decimal)).AllowDBNull = true;
            dtTPStats.Columns.Add("GC Old Space Change (mb)", typeof(decimal)).AllowDBNull = true; //p
            dtTPStats.Columns.Add("IORate (mb/sec)", typeof(decimal)).AllowDBNull = true; //q
            dtTPStats.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;//R

            /*
            dtTPStats.DefaultView.ApplyDefaultSort = false;
            dtTPStats.DefaultView.AllowDelete = false;
            dtTPStats.DefaultView.AllowEdit = false;
            dtTPStats.DefaultView.AllowNew = false;
            dtTPStats.DefaultView.Sort = string.Format("[{0}] ASC, [Name] ASC", ColumnNames.KeySpace);
            */

            return dtTPStats;
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
                var statCollection = this.Cluster.Nodes.SelectMany(d => d.AggregatedStats.Where(i => i.Source == DSEDiagnosticLibrary.SourceTypes.TPStats));
                DSEDiagnosticLibrary.UnitOfMeasure uom = null;
                
                Logger.Instance.InfoFormat("Loading {0} NodeStats", statCollection.Count());

                foreach (var stat in statCollection)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    var tpStatGroups = from statItem in stat.Data
                                       let attrKeyCol = this.GetColumnNameFromAttributeKey(statItem.Key)
                                       group new { Col = attrKeyCol.Item2, Value = statItem.Value } by attrKeyCol.Item1 into g
                                       select new { Attr = g.Key, Values = g };

                    foreach (var item in tpStatGroups)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        dataRow.SetField(ColumnNames.Source, stat.Source.ToString());
                        dataRow.SetField(ColumnNames.DataCenter, stat.DataCenter.Name);
                        dataRow.SetField(ColumnNames.NodeIPAddress, stat.Node.Id.NodeName());

                        dataRow.SetField("Attribute", item.Attr);

                        foreach (var itemValue in item.Values)
                        {
                            dataRow.SetField(itemValue.Col, itemValue.Value);
                        }

                        if (stat.ReconciliationRefs.HasAtLeastOneElement())
                        {
                            dataRow.SetField(ColumnNames.ReconciliationRef,
                                                stat.ReconciliationRefs.IsMultiple()
                                                    ? (object)string.Join(",", stat.ReconciliationRefs)
                                                    : (object)stat.ReconciliationRefs.First());
                        }

                        this.Table.Rows.Add(dataRow);

                        ++nbrItems;
                    }
                }

                Logger.Instance.InfoFormat("Loaded {0:###,###,##0} NodeStats", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading NodeStats Canceled");
            }
            finally
            {
                this.Table.AcceptChanges();
                this.Table.EndLoadData();
            }

            return this.Table;
        }

        /// <summary>
        /// Pool Name                    Active   Pending      Completed   Blocked  All time blocked
        ///  Attribute Key: Pool Name.MutationStage.Active
        ///  Message type           Dropped
        ///  Attribute Key: READ.Dropped
        ///
        /// </summary>
        /// <param name="attribute"></param>
        /// <returns>
        /// Item1: Attribute Name
        /// Item2: Column Name
        /// </returns>
        Tuple<string,string> GetColumnNameFromAttributeKey(string attributeKey)
        {
            var splitNames = attributeKey.Split('.');

            return new Tuple<string, string>(splitNames[splitNames.Length - 2], splitNames.Last());
        }
    }
}
