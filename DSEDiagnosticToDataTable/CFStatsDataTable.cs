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
    public sealed class CFStatsDataTable : DataTableLoad
    {
        public CFStatsDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, string[] warnWhenKSTblIsDetected = null)
            : base(cluster, cancellationSource)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
            this.WarnWhenKSTblIsDetected = warnWhenKSTblIsDetected ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }
        public string[] WarnWhenKSTblIsDetected { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtCFStats = new DataTable(TableNames.CFStats, TableNames.Namespace);

            dtCFStats.Columns.Add(ColumnNames.Source, typeof(string)); //A
            dtCFStats.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtCFStats.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtCFStats.Columns.Add(ColumnNames.KeySpace, typeof(string));
            dtCFStats.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;
            dtCFStats.Columns.Add("Attribute", typeof(string)); //F
            dtCFStats.Columns.Add("Value", typeof(object));
            dtCFStats.Columns.Add("Unit of Measure", typeof(string)).AllowDBNull = true;

            dtCFStats.Columns.Add("Size in MB", typeof(decimal)).AllowDBNull = true; //I
            dtCFStats.Columns.Add("Active", typeof(bool)).AllowDBNull = true;
            dtCFStats.Columns.Add(ColumnNames.ReconciliationRef, typeof(object)).AllowDBNull = true; //K

            /*
            dtCFStats.DefaultView.ApplyDefaultSort = false;
            dtCFStats.DefaultView.AllowDelete = false;
            dtCFStats.DefaultView.AllowEdit = false;
            dtCFStats.DefaultView.AllowNew = false;
            dtCFStats.DefaultView.Sort = string.Format("[{0}] ASC, [Name] ASC", ColumnNames.KeySpace);
            */

            return dtCFStats;
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
                var statCollection = this.Cluster.Nodes.SelectMany(d => d.AggregatedStats.Where(i => i.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.KeyspaceTableViewIndexStats | DSEDiagnosticLibrary.EventClasses.Node)));
                DSEDiagnosticLibrary.UnitOfMeasure uom = null;
                bool warn = false;

                Logger.Instance.InfoFormat("Loading {0} CFStats", statCollection.Count());

                foreach (var stat in statCollection)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();
                    warn = false;

                    if (stat.Keyspace != null && this.WarnWhenKSTblIsDetected.Any(n => n == stat.Keyspace.Name || (stat.TableViewIndex != null && stat.TableViewIndex.FullName == n)))
                    {
                        warn = stat.Data.Any(s => ((stat.TableViewIndex == null && (s.Key == "Read Count" || s.Key == "Write Count"))
                                                    || (stat.TableViewIndex != null && (s.Key == "Local read count" || s.Key == "Local write count"))) && (dynamic)s.Value > 0);
                    }

                    if (!warn && stat.Keyspace != null && this.IgnoreKeySpaces.Any(n => n == stat.Keyspace.Name))
                    {
                        continue;
                    }

                    foreach (var item in stat.Data)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        dataRow.SetField(ColumnNames.Source, stat.Source.ToString());
                        dataRow.SetField(ColumnNames.DataCenter, stat.DataCenter.Name);
                        dataRow.SetField(ColumnNames.NodeIPAddress, stat.Node.Id.NodeName());
                        dataRow.SetField(ColumnNames.KeySpace, warn ? stat.Keyspace.Name + " (warn)" : stat.Keyspace.Name);

                        if (stat.TableViewIndex != null)
                        {
                            dataRow.SetField(ColumnNames.Table, warn ? stat.TableViewIndex.Name + " (warn)" : stat.TableViewIndex.Name);
                            dataRow.SetField("Active", stat.TableViewIndex.IsActive);
                        }

                        dataRow.SetField("Attribute", item.Key);

                        uom = item.Value as DSEDiagnosticLibrary.UnitOfMeasure;

                        if(uom == null)
                        {
                            dataRow.SetField("Value", item.Value);
                        }
                        else
                        {
                            if(uom.Value % 1 == 0)
                            {
                                dataRow.SetFieldToLong("Value", uom);
                            }
                            else
                            {
                                dataRow.SetFieldToDecimal("Value", uom);
                            }

                            dataRow.SetField("Unit of Measure", uom.UnitType.ToString());

                            if((uom.UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.SizeUnits) != 0)
                            {
                                dataRow.SetField("Size in MB", uom.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB));
                            }
                        }

                        if (stat.ReconciliationRefs.HasAtLeastOneElement())
                        {
                            dataRow.SetField(ColumnNames.ReconciliationRef,
                                                stat.ReconciliationRefs.IsMultiple()
                                                    ? (object) string.Join(",", stat.ReconciliationRefs)
                                                    : (object) stat.ReconciliationRefs.First());
                        }

                        this.Table.Rows.Add(dataRow);

                        ++nbrItems;
                    }
                }

                Logger.Instance.InfoFormat("Loaded {0:###,###,##0} CFStats", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading CFStats Canceled");
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
