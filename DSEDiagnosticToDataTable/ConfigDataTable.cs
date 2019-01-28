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
    public sealed class ConfigDataTable : DataTableLoad
    {

        public ConfigDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {}

        public override DataTable CreateInitializationTable()
        {
            var dtConfig = new DataTable(TableNames.Config, TableNames.Namespace);
           
            if(this.SessionId.HasValue) dtConfig.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtConfig.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtConfig.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtConfig.Columns.Add("Yaml Type", typeof(string));
            dtConfig.Columns.Add("Property", typeof(string));
            dtConfig.Columns.Add("Value", typeof(string));

            //dtConfig.PrimaryKey = new System.Data.DataColumn[] { dtConfig.Columns[ColumnNames.DataCenter], dtConfig.Columns[ColumnNames.NodeIPAddress], dtConfig.Columns["Yaml Type"], dtConfig.Columns["Property"] };

            dtConfig.DefaultView.ApplyDefaultSort = false;
            dtConfig.DefaultView.AllowDelete = false;
            dtConfig.DefaultView.AllowEdit = false;
            dtConfig.DefaultView.AllowNew = false;
            dtConfig.DefaultView.Sort = string.Format("[Yaml Type] ASC, [Property] ASC, [{0}] ASC, [{1}] ASC", ColumnNames.DataCenter, ColumnNames.NodeIPAddress);

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
                int nbrItems = 0;
                
                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading DSE Configuration for DC \"{0}\"", dataCenter.Name);

                    /* Type             Prop                ValueCnt    {GrpValue}
                    //  Cassandra.Yaml  commitlog_directory 2           Value                                   ValueCnt    TotalNodes Nodes
                    //                                                  /xnet2/dse-node1/cassandra/commitlog    1           2           {node1, node2}
                    //                                                  /xnet4/dse-node2/cassandra/commitlog    2           4           {node3, node4, node5, node6}
                    //                                                  /xnet4/dse-node2/cassandra/commitlog    2           6           {node7 node8, node9, node10, node11, node12}
                    */
                    var groupedTPConfLines = from configLine in dataCenter.Configurations
                                             let normProp = configLine.Source == DSEDiagnosticLibrary.SourceTypes.EnvFile 
                                                                ? configLine.Property
                                                                : configLine.NormalizeProperty()
                                             group configLine by new { Type = configLine.Type.ToString() + '.' + configLine.Source.ToString(),
                                                 Prop = normProp } into gtp
                                             select new
                                             {
                                                 Type = gtp.Key.Type,
                                                 Prop = gtp.Key.Prop,
                                                 ValueCnt = gtp.Count(),
                                                 GrpValues = (from valueItem in gtp
                                                              group valueItem by valueItem.NormalizeValue() into gv
                                                              let vCnt = gv.Count()      
                                                              let nodes = gv.SelectMany(c => c.DataCenter is DSEDiagnosticLibrary.DataCenter
                                                                                                    ? new DSEDiagnosticLibrary.INode[] { c.Node }
                                                                                                    : c.DataCenter.Nodes).DuplicatesRemoved(n => n)
                                                              select new { Value = gv.Key,
                                                                            ValueCnt = vCnt,                                                                  
                                                                            TotalNodes = nodes.Count(),
                                                                            Nodes = nodes
                                                                            })
                                                                .OrderByDescending(i => i.TotalNodes)
                                             };
                    var dcNodeCnt = dataCenter.Nodes.Count();

                    foreach (var groupItem in groupedTPConfLines)
                    {                       
                        this.CancellationToken.ThrowIfCancellationRequested();

                        var nbrGrpValues = groupItem.GrpValues.Count();
                        for (int nIdx = 0; nIdx < nbrGrpValues; nIdx++)
                        {
                            this.CancellationToken.ThrowIfCancellationRequested();

                            var grpValueItem = groupItem.GrpValues.ElementAt(nIdx);
                            var dataRow = this.Table.NewRow();

                            if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                            dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);

                            if (nbrGrpValues == 1 && grpValueItem.TotalNodes == 1)
                            {
                                dataRow.SetField(ColumnNames.NodeIPAddress, grpValueItem.Nodes.First().Id.NodeName());
                            }
                            else if (grpValueItem.TotalNodes == dcNodeCnt)
                            {
                                dataRow.SetField(ColumnNames.NodeIPAddress, "<Common All Nodes in DC>");
                            }
                            else if (nbrGrpValues == 1)
                            {
                                dataRow.SetField(ColumnNames.NodeIPAddress, string.Format("<Partial ({0})>: ", grpValueItem.TotalNodes)
                                                                                + string.Join(", ", grpValueItem.Nodes.Select(n => n.Id.NodeName()).OrderBy(hn => hn)));
                            }
                            else if (nIdx == 0
                                        && grpValueItem.Nodes.Count() > groupItem.GrpValues.ElementAt(1).Nodes.Count())
                            {
                                dataRow.SetField(ColumnNames.NodeIPAddress, string.Format("<Common Majority Nodes ({0}) in DC>", grpValueItem.Nodes.Count()));
                            }
                            else
                            {
                                dataRow.SetField(ColumnNames.NodeIPAddress, string.Join(", ", grpValueItem.Nodes.Select(n => n.Id.NodeName()).OrderBy(hn => hn)));
                            }

                            dataRow.SetField("Yaml Type", groupItem.Type);
                            dataRow.SetField("Property", groupItem.Prop);
                            dataRow.SetFieldStringLimit("Value", grpValueItem.Value);

                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;                            
                        }
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
