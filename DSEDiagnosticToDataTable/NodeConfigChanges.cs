using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLogger;
using Common;

namespace DSEDiagnosticToDataTable
{
    public class NodeConfigChanges : DataTableLoad
    {
        public NodeConfigChanges(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        { }

        public override DataTable CreateInitializationTable()
        {
            var dtConfig = new DataTable(TableNames.NodeConfigChanges, TableNames.Namespace);

            if (this.SessionId.HasValue) dtConfig.Columns.Add(ColumnNames.SessionId, typeof(Guid));
            
            dtConfig.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtConfig.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtConfig.Columns.Add(ColumnNames.UTCTimeStamp, typeof(DateTime));
            dtConfig.Columns.Add(ColumnNames.LogLocalTimeStamp, typeof(DateTime)).AllowDBNull = true;
            dtConfig.Columns.Add(ColumnNames.LogLocalTZOffset, typeof(string)).AllowDBNull = true;
            dtConfig.Columns.Add("Type", typeof(string));
            dtConfig.Columns.Add("Property", typeof(string));
            dtConfig.Columns.Add("Value", typeof(string));
            dtConfig.Columns.Add("Current Value", typeof(string));

            //dtConfig.PrimaryKey = new System.Data.DataColumn[] { dtConfig.Columns[ColumnNames.DataCenter], dtConfig.Columns[ColumnNames.NodeIPAddress], dtConfig.Columns["UTC Timestamp"], dtConfig.Columns["Type"], dtConfig.Columns["Property"] };

            dtConfig.DefaultView.ApplyDefaultSort = false;
            dtConfig.DefaultView.AllowDelete = false;
            dtConfig.DefaultView.AllowEdit = false;
            dtConfig.DefaultView.AllowNew = false;
            dtConfig.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [UTC Timestamp] ASC, [Type] ASC, [Property] ASC", ColumnNames.DataCenter, ColumnNames.NodeIPAddress);

            return dtConfig;
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
                var parallelOptions = new ParallelOptions();

                if(this.CancellationToken != null) parallelOptions.CancellationToken = this.CancellationToken; //Do not set since this will throw and NOT properly caught resulting in the consumer getting the throw
                parallelOptions.MaxDegreeOfParallelism = System.Environment.ProcessorCount;

                System.Threading.Tasks.Parallel.ForEach(this.Cluster.Nodes, parallelOptions, (node, loopState) =>
                {
                   nbrItems = 0;
                   this.CancellationToken.ThrowIfCancellationRequested();

                   Logger.Instance.InfoFormat("Checking Node Configuration Changes from Logs for \"{0}\"", node.Id.NodeName());

                   var logConfigLines = from logEvent in node.LogEvents.AsParallel()
                                        let searchEvent = logEvent.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView)
                                        where (searchEvent.Class & DSEDiagnosticLibrary.EventClasses.Config) != 0
                                               && (searchEvent.Class & DSEDiagnosticLibrary.EventClasses.NodeDetection) != 0
                                        select logEvent.Value;
                   IEnumerable<DSEDiagnosticLibrary.IConfigurationLine> nodeConfigs = null;

                   if (logConfigLines.HasAtLeastOneElement())
                   {
                       nodeConfigs = node.Configurations.Where(c => c.Source == DSEDiagnosticLibrary.SourceTypes.Yaml);
                   }

                   foreach (var logConfigLine in logConfigLines)
                   {
                       this.CancellationToken.ThrowIfCancellationRequested();

                       foreach (var logConfigItem in logConfigLine.LogProperties)
                       {
                           this.CancellationToken.ThrowIfCancellationRequested();

                           var currMatchItem = nodeConfigs.FirstOrDefault(c => c.PropertyName() == logConfigItem.Key);
                           string currValue = null;
                           string logConfigValue;
                           bool matched = false;

                           if (logConfigItem.Value is DSEDiagnosticLibrary.UnitOfMeasure)
                           {
                               logConfigValue = DSEDiagnosticLibrary.StringHelpers.DetermineProperFormat(((DSEDiagnosticLibrary.UnitOfMeasure)logConfigItem.Value).Value.ToString());
                           }
                           else
                           {
                               logConfigValue = DSEDiagnosticLibrary.StringHelpers.DetermineProperFormat(logConfigItem.Value?.ToString());
                           }

                           if (string.IsNullOrEmpty(logConfigValue))
                           {
                               continue;
                           }

                           if (currMatchItem != null)
                           {
                               currValue = currMatchItem.NormalizeValue();
                               matched = currValue == logConfigValue;
                           }

                           if (!matched)
                           {
                               dataRow = this.Table.NewRow();

                               if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                               dataRow.SetField(ColumnNames.DataCenter, node.DataCenter.Name);
                               dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                               dataRow.SetField("UTC Timestamp", logConfigLine.EventTime.UtcDateTime);
                               dataRow.SetField("Log Local Timestamp", logConfigLine.EventTime.DateTime);
                               dataRow.SetFieldToTZOffset("Log Time Zone Offset", logConfigLine.EventTime);
                               dataRow.SetField("Type", logConfigLine.Source);
                               dataRow.SetField("Property", currMatchItem?.Property ?? logConfigItem.Key);
                               dataRow.SetField("Value", logConfigValue);
                               dataRow.SetField("Current Value", currValue ?? "<Property Missing or Could not be Determined>");

                               this.Table.Rows.Add(dataRow);
                           }

                       }
                       ++nbrItems;
                   }

                   Logger.Instance.InfoFormat("Node Configuration Change Processing for node \"{0}\" completed, Total Nbr Items {1:###,###,##0}", node.Id.NodeName(), nbrItems);
                });
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Node Log Configuration Processing Canceled");
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
