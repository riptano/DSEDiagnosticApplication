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
        public TPStatsDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
        }

        public override DataTable CreateInitializationTable()
        {
            var dtTPStats = new DataTable(TableNames.NodeStats, TableNames.Namespace);
            
            if (this.SessionId.HasValue) dtTPStats.Columns.Add(ColumnNames.SessionId, typeof(Guid));

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
            
            dtTPStats.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;//r

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
                var statCollection = this.Cluster.Nodes.SelectMany(d => d.AggregatedStats.Where(i => (i.Class & DSEDiagnosticLibrary.EventClasses.NodeStats) != 0
                                                                                                      && (i.Class & DSEDiagnosticLibrary.EventClasses.Keyspace) == 0
                                                                                                      && (i.Class & DSEDiagnosticLibrary.EventClasses.TableViewIndex) == 0));

                Logger.Instance.InfoFormat("Loading {0} NodeStats", statCollection.Count());

                foreach (var stat in statCollection)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    var tpStatGroups = from statItem in stat.Data
                                       let attrKeyCol = GetColumnNameFromAttributeKey(statItem.Key)
                                       group new { Col = attrKeyCol.Item2, Value = statItem.Value } by attrKeyCol.Item1 into g
                                       select new { Attr = g.Key, Values = g };

                    foreach (var item in tpStatGroups)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow.SetField(ColumnNames.Source, stat.Source.ToString());
                        dataRow.SetField(ColumnNames.DataCenter, stat.DataCenter.Name);
                        dataRow.SetField(ColumnNames.NodeIPAddress, stat.Node.NodeName());

                        dataRow.SetField("Attribute", item.Attr);

                        foreach (var itemValue in item.Values)
                        {
                            if (itemValue.Value is DSEDiagnosticLibrary.UnitOfMeasure)
                            {
                                dataRow.SetFieldToDecimal(itemValue.Col, (DSEDiagnosticLibrary.UnitOfMeasure)itemValue.Value, DSEDiagnosticLibrary.UnitOfMeasure.Types.MS);
                            }
                            else
                            {
                                dataRow.SetField(itemValue.Col, itemValue.Value);
                            }
                        }

                        if (stat.ReconciliationRefs.HasAtLeastOneElement())
                        {
                            dataRow.SetFieldStringLimit(ColumnNames.ReconciliationRef,
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
        ///     Attribute Key: Pool Name.MutationStage.Active
        ///     Attribute Name: MutationStage Column Name: Active
        ///
        ///  Message type           Dropped
        ///     Attribute Key: READ.Dropped
        ///     Attribute Name: READ Column Name: Dropped
        ///
        /// Message type           Dropped                  Latency waiting in queue (micros)                                    
        ///                                                 50%               95%               99%               Max
        ///     Attribute Key: READ.Dropped
        ///     Attribute Name: READ Column Name: Dropped
        ///     Attribute Key: READ.Latency.Waiting.50%
        ///     Attribute Name: READ.Latency.Waiting.50% Column Name: Latency (ms)
        /// </summary>
        /// <param name="attribute"></param>
        /// <returns>
        /// Item1: Attribute Name
        /// Item2: Column Name
        /// </returns>
        public static Tuple<string,string> GetColumnNameFromAttributeKey(string attributeKey)
        {
            var splitNames = attributeKey.Split('.');
            var attrName = splitNames[splitNames.Length - 2];
            var colName = splitNames.Last();

            if (splitNames.Length == 4 && attrName == "Waiting")
            {
                attrName = attributeKey;
                colName = "Latency (ms)";
            }

            return new Tuple<string, string>(attrName, colName);
        }
    }
}
