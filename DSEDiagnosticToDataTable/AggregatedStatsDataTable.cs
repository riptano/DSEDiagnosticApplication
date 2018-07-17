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
    public sealed class AggregatedStatsDataTable : DataTableLoad
    {
        public AggregatedStatsDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, string[] ignoreKeySpaces = null, string[] warnWhenKSTblIsDetected = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
            this.IgnoreKeySpaces = ignoreKeySpaces ?? new string[0];
            this.WarnWhenKSTblIsDetected = warnWhenKSTblIsDetected ?? new string[0];
        }

        public string[] IgnoreKeySpaces { get; }
        public string[] WarnWhenKSTblIsDetected { get; }

        public override DataTable CreateInitializationTable()
        {
            var dtStats = new DataTable(TableNames.AggregatedStats, TableNames.Namespace);

            if(this.SessionId.HasValue) dtStats.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtStats.Columns.Add(ColumnNames.Source, typeof(string)); //A
            dtStats.Columns.Add("Type", typeof(string)); //B
            dtStats.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtStats.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtStats.Columns.Add(ColumnNames.KeySpace, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add("CQL Type", typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add("Properties", typeof(string)).AllowDBNull = true;
            dtStats.Columns.Add("Attribute", typeof(string)); //I
            dtStats.Columns.Add("Value", typeof(object));
            dtStats.Columns.Add("NumericValue", typeof(object));
            dtStats.Columns.Add("Unit of Measure", typeof(string)).AllowDBNull = true;

            dtStats.Columns.Add("Raw Value", typeof(object)).AllowDBNull = true; //L
            dtStats.Columns.Add("Active", typeof(bool)).AllowDBNull = true;
            dtStats.Columns.Add(ColumnNames.ReconciliationRef, typeof(object)).AllowDBNull = true; //N
            
            /*
            dtCFStats.DefaultView.ApplyDefaultSort = false;
            dtCFStats.DefaultView.AllowDelete = false;
            dtCFStats.DefaultView.AllowEdit = false;
            dtCFStats.DefaultView.AllowNew = false;
            dtCFStats.DefaultView.Sort = string.Format("[{0}] ASC, [Name] ASC", ColumnNames.KeySpace);
            */

            return dtStats;
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
                var statCollection = this.Cluster.Nodes
                                        .SelectMany(d => d.AggregatedStats);
                bool warn = false;

                Logger.Instance.InfoFormat("Loading {0} Aggregated Stats", statCollection.Count());

                foreach (var stat in statCollection)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();
                    warn = false;

                    if (stat.Keyspace != null && this.WarnWhenKSTblIsDetected.Any(n => n == stat.Keyspace.Name || (stat.TableViewIndex != null && stat.TableViewIndex.FullName == n)))
                    {
                        warn = stat.Data.Any(s => ((stat.TableViewIndex == null
                                                                && (s.Key == Properties.Settings.Default.CFStatsReadCountName
                                                                        || s.Key == Properties.Settings.Default.CFStatsWriteCountName))
                                                        || (stat.TableViewIndex != null
                                                                && (s.Key == Properties.Settings.Default.CFStatsLocalReadCountName
                                                                        || s.Key == Properties.Settings.Default.CFStatsLocalWriteCountName)))
                                                    && (dynamic)s.Value > 0);
                    }

                    if (!warn && stat.Keyspace != null && this.IgnoreKeySpaces.Any(n => n == stat.Keyspace.Name))
                    {
                        continue;
                    }

                    var keyspaceName = warn ? stat.Keyspace.Name + " (warn)" : stat.Keyspace?.Name;
                    
                    {
                        object errorValue;

                        if (stat.Data.TryGetValue(DSEDiagnosticLibrary.AggregatedStats.DCNotInKS, out errorValue))
                        {
                            keyspaceName += string.Format(" {{!{0}}}", errorValue);
                            dataRow = this.Table.NewRow();

                            if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                            dataRow.SetField(ColumnNames.Source, stat.Source.ToString());
                            dataRow.SetField(ColumnNames.DataCenter, stat.DataCenter.Name);
                            dataRow.SetField(ColumnNames.NodeIPAddress, stat.Node.Id.NodeName());
                            dataRow.SetField(ColumnNames.KeySpace, keyspaceName);
                            dataRow.SetField("Type", stat.Class.ToString());

                            if (stat.TableViewIndex != null)
                            {
                                this.SetTableIndexInfo(dataRow, stat, warn);
                            }

                            dataRow.SetField("Attribute", DSEDiagnosticLibrary.AggregatedStats.DCNotInKS);
                            dataRow.SetField("Value",
                                                string.Format("{0} not found within Keyspace \"{1}\"",
                                                                errorValue.ToString(),
                                                                stat.Keyspace.Name));

                            this.Table.Rows.Add(dataRow);

                            ++nbrItems;
                        }                                         
                    }

                    foreach (var item in stat.Data)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if(item.Key == DSEDiagnosticLibrary.AggregatedStats.DCNotInKS)
                        {
                            continue;
                        }

                        if (item.Key.StartsWith(DSEDiagnosticLibrary.AggregatedStats.Error))
                        {
                            foreach (var strError in (IList<string>)item.Value)
                            {
                                this.CancellationToken.ThrowIfCancellationRequested();

                                dataRow = this.Table.NewRow();

                                if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                                dataRow.SetField(ColumnNames.Source, stat.Source.ToString());
                                dataRow.SetField(ColumnNames.DataCenter, stat.DataCenter.Name);
                                dataRow.SetField(ColumnNames.NodeIPAddress, stat.Node.Id.NodeName());
                                dataRow.SetField(ColumnNames.KeySpace, keyspaceName);
                                dataRow.SetField("Type", stat.Class.ToString());

                                if (stat.TableViewIndex != null)
                                {
                                    this.SetTableIndexInfo(dataRow, stat, warn);
                                }

                                dataRow.SetField("Attribute", item.Key);
                                dataRow.SetField("Value", strError);

                                this.Table.Rows.Add(dataRow);

                                ++nbrItems;
                            }

                            continue;
                        }

                        nbrItems += this.AddAggregatedDataRow(stat, warn, keyspaceName, item.Key, item.Value);
                    }
                }

                Logger.Instance.InfoFormat("Loaded {0:###,###,##0} Aggregated Stats", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Aggregated Stats Canceled");
            }
            finally
            {
                this.Table.AcceptChanges();
                this.Table.EndLoadData();
            }

            return this.Table;
        }

        private int AddAggregatedDataRow(DSEDiagnosticLibrary.IAggregatedStats stat, bool warn, string keyspaceName, string propName, object propValue)
        {           
            if(!(propValue is string) && propValue is System.Collections.IEnumerable)
            {
                int nbrItems = 0;

                foreach(var lstItem in (System.Collections.IEnumerable) propValue)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();
                    nbrItems += this.AddAggregatedDataRow(stat, warn, keyspaceName, propName, lstItem);
                }

                return nbrItems;
            }

            var dataRow = this.Table.NewRow();

            if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

            dataRow.SetField(ColumnNames.Source, stat.Source.ToString());
            dataRow.SetField(ColumnNames.DataCenter, stat.DataCenter.Name);
            dataRow.SetField(ColumnNames.NodeIPAddress, stat.Node.Id.NodeName());
            dataRow.SetField(ColumnNames.KeySpace, keyspaceName);
            dataRow.SetField("Type", stat.Class.ToString());

            if (stat.TableViewIndex != null)
            {                
                this.SetTableIndexInfo(dataRow, stat, warn);
            }

            dataRow.SetField("Attribute", propName);

            if (propValue is DSEDiagnosticLibrary.UnitOfMeasure)
            {
                DSEDiagnosticLibrary.UnitOfMeasure uom = (DSEDiagnosticLibrary.UnitOfMeasure)propValue;
                if (uom.Value % 1 == 0)
                {
                    dataRow.SetFieldToLong("Raw Value", uom);                    
                }
                else
                {
                    dataRow.SetFieldToDecimal("Raw Value", uom);                    
                }

                dataRow.SetField("Unit of Measure", uom.UnitType.ToString());

                if (!uom.NaN && (uom.UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.SizeUnits) != 0)
                {
                    dataRow.SetField("Value", uom.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB));                       
                }
                else
                {
                    dataRow.SetField("Value", dataRow["Raw Value"]);
                }
                dataRow.SetField("NumericValue", dataRow["Value"]);
            }
            else
            {
                dataRow.SetField("Value", propValue);

                if(DSEDiagnosticLibrary.MiscHelpers.IsNumber(propValue)) dataRow.SetField("NumericValue", propValue);
            }

            if (stat.ReconciliationRefs.HasAtLeastOneElement())
            {
                dataRow.SetFieldStringLimit(ColumnNames.ReconciliationRef,
                                                stat.ReconciliationRefs.IsMultiple()
                                                        ? (object)string.Join(",", stat.ReconciliationRefs)
                                                        : (object)stat.ReconciliationRefs.First());
            }

            this.Table.Rows.Add(dataRow);

            return 1;
        }

        private void SetTableIndexInfo(DataRow dataRow, DSEDiagnosticLibrary.IAggregatedStats stat, bool warn)
        {            
            dataRow.SetField("Active", stat.TableViewIndex.IsActive);

            if((stat.Class & DSEDiagnosticLibrary.EventClasses.Solr) != 0 && stat.TableViewIndex is DSEDiagnosticLibrary.ICQLTable)
                dataRow.SetField("CQL Type", "SolrIndex");
            else 
                dataRow.SetField("CQL Type", stat.TableViewIndex.GetType().Name);
            if (stat.TableViewIndex is DSEDiagnosticLibrary.ICQLIndex)
            {
                var cqlIndex = (DSEDiagnosticLibrary.ICQLIndex)stat.TableViewIndex;
                
                dataRow.SetField("Properties", cqlIndex.Table.Compaction);
                dataRow.SetField(ColumnNames.Table, warn ? cqlIndex.Table.Name + '.' + cqlIndex.Name + " (warn)" : cqlIndex.Table.Name + '.' + cqlIndex.Name);                
            }
            else if (stat.TableViewIndex is DSEDiagnosticLibrary.ICQLTable)
            {
                dataRow.SetField("Properties", ((DSEDiagnosticLibrary.ICQLTable)stat.TableViewIndex).Compaction);
                dataRow.SetField(ColumnNames.Table, warn ? stat.TableViewIndex.Name + " (warn)" : stat.TableViewIndex.Name);
            }
            else
                dataRow.SetField(ColumnNames.Table, warn ? stat.TableViewIndex.Name + " (warn)" : stat.TableViewIndex.Name);
        }
    }
}
