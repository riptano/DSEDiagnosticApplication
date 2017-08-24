using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class ConfigDataTable : DataTableLoad
    {

        public ConfigDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null)
            : base(cluster, cancellationSource)
        {}
       
        public override DataTable CreateInitializationTable()
        {
            var dtConfig = new DataTable("DSEConfiguration", "DSEDiagnostic");

            dtConfig.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtConfig.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtConfig.Columns.Add("Yaml Type", typeof(string));
            dtConfig.Columns.Add("Property", typeof(string));
            dtConfig.Columns.Add("Value", typeof(string));

            dtConfig.PrimaryKey = new System.Data.DataColumn[] { dtConfig.Columns[ColumnNames.DataCenter], dtConfig.Columns[ColumnNames.NodeIPAddress], dtConfig.Columns["Yaml Type"], dtConfig.Columns["Property"] };

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

                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading DSE Configuration for DC \"{0}\"", dataCenter.Name);

                    foreach (var configItem in dataCenter.Configurations)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        if(this.Table.Rows.Contains(new object[] { dataCenter.Name, "<Common>", configItem.Type.ToString(), configItem.NormalizeProperty() }))
                        {
                            continue;
                        }

                        dataRow = this.Table.NewRow();

                        dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);

                        if(configItem.IsCommonToDataCenter)
                        {
                            dataRow.SetField(ColumnNames.NodeIPAddress, "<Common>");
                        }
                        else
                        {
                            dataRow.SetField(ColumnNames.NodeIPAddress, configItem.Node.Id.NodeName());
                        }
                        dataRow.SetField("Yaml Type", configItem.Type.ToString());
                        dataRow.SetField("Property", configItem.NormalizeProperty());
                        dataRow.SetField("Value", configItem.NormalizeValue());

                        this.Table.Rows.Add(dataRow);
                        ++nbrItems;
                    }

                    Logger.Instance.InfoFormat("Loaded DSE Configuration for DC \"{0}\", Total Nbr Items {1:###,###,##0}", dataCenter.Name, nbrItems);
                }
            }
            catch(OperationCanceledException)
            {
                Logger.Instance.Warn("Loading DSE Configuration Canceled");
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
