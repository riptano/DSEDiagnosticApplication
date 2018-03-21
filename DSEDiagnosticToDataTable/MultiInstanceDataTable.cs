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
    public class MultiInstanceDataTable : DataTableLoad
    {
        public MultiInstanceDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        { }

        public override DataTable CreateInitializationTable()
        {
            var dtMultiInstanceInfo = new DataTable(TableNames.MultiInstanceInfo, TableNames.Namespace);

            if (this.SessionId.HasValue) dtMultiInstanceInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtMultiInstanceInfo.Columns.Add("Multi-Instance Server Id", typeof(string));  //A          
            dtMultiInstanceInfo.Columns.Add(ColumnNames.DataCenter, typeof(string)); //B
            dtMultiInstanceInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)); //C
            dtMultiInstanceInfo.Columns[ColumnNames.NodeIPAddress].Unique = true;
            dtMultiInstanceInfo.PrimaryKey = new System.Data.DataColumn[] { dtMultiInstanceInfo.Columns["Multi-Instance Server Id"], dtMultiInstanceInfo.Columns[ColumnNames.NodeIPAddress] };

            
            dtMultiInstanceInfo.Columns.Add("Rack", typeof(string));            
            dtMultiInstanceInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true; //E
            dtMultiInstanceInfo.Columns.Add("Host Names", typeof(string)).AllowDBNull = true; //F

            dtMultiInstanceInfo.DefaultView.ApplyDefaultSort = false;
            dtMultiInstanceInfo.DefaultView.AllowDelete = false;
            dtMultiInstanceInfo.DefaultView.AllowEdit = false;
            dtMultiInstanceInfo.DefaultView.AllowNew = false;
            dtMultiInstanceInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [{2}] ASC, [{3}] ASC", "Multi-Instance Server Id", ColumnNames.DataCenter, ColumnNames.NodeIPAddress, "Rack");

            return dtMultiInstanceInfo;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        public override DataTable LoadTable()
        {
            var multiInstanceNodes = this.Cluster.Nodes.Where(n => (n.DSE.InstanceType & DSEDiagnosticLibrary.DSEInfo.InstanceTypes.MultiInstance) != 0
                                                                    && !string.IsNullOrEmpty(n.DSE.PhysicalServerId));

            if (multiInstanceNodes.IsEmpty()) return this.Table;

            this.Table.BeginLoadData();
            try
            {
                DataRow dataRow = null;
                int nbrItems = 0;

                Logger.Instance.InfoFormat("Loading Multi-Instance Information for {0} nodes", multiInstanceNodes.Count());

                foreach (var node in multiInstanceNodes)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    dataRow = this.Table.NewRow();

                    if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                    dataRow.SetField("Multi-Instance Server Id", node.DSE.PhysicalServerId);
                    dataRow.SetField(ColumnNames.DataCenter, node.DataCenter.Name);
                    dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                    dataRow.SetField("Rack", node.DSE.Rack);
                    dataRow.SetField("Instance Type", node.DSE.InstanceType.ToString());
                    dataRow.SetField("Host Names", node.Id.HostNames.IsEmpty() ? null : string.Join(", ", node.Id.HostNames));

                    this.Table.Rows.Add(dataRow);
                    ++nbrItems;                    
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Multi-Instance Information Canceled");
            }
            finally
            {
                this.Table.AcceptChanges();
                this.Table.EndLoadData();
            }

            Logger.Instance.InfoFormat("Loaded Multi-Instance Information for {0} nodes", multiInstanceNodes.Count());

            return this.Table;
        }
    }
}
