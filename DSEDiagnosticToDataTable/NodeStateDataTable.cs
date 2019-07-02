using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
using Common;


namespace DSEDiagnosticToDataTable
{
    public sealed class NodeStateDataTable : DataTableLoad
    {
        public NodeStateDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {
        }

        public struct Columns
        {
            public const string State = "State";
            public const string Duration = "Duration";
            public const string RelatedInfo = "Related Info";
            public const string SourceNode = "Source Node";
            public const string SortOrder = "Sort Order";
        }

        public override DataTable CreateInitializationTable()
        {
            var dtNodeInfo = new DataTable(TableNames.NodeState, TableNames.Namespace);

            if (this.SessionId.HasValue) dtNodeInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtNodeInfo.Columns.Add(ColumnNames.UTCTimeStamp, typeof(DateTime));
            dtNodeInfo.Columns.Add(ColumnNames.LogLocalTimeStamp, typeof(DateTime)).AllowDBNull = true;
            dtNodeInfo.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtNodeInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtNodeInfo.Columns.Add(Columns.SortOrder, typeof(Int16));
            dtNodeInfo.Columns.Add(Columns.State, typeof(string));
            dtNodeInfo.Columns.Add(Columns.Duration, typeof(TimeSpan)).AllowDBNull = true;
            dtNodeInfo.Columns.Add(Columns.SourceNode, typeof(string)).AllowDBNull = true;

            dtNodeInfo.DefaultView.ApplyDefaultSort = false;
            dtNodeInfo.DefaultView.AllowDelete = false;
            dtNodeInfo.DefaultView.AllowEdit = false;
            dtNodeInfo.DefaultView.AllowNew = false;
            dtNodeInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [{2}] ASC, [{3}] ASC",
                                                    ColumnNames.UTCTimeStamp,
                                                    ColumnNames.DataCenter,
                                                    ColumnNames.NodeIPAddress,
                                                    Columns.SortOrder);                                                    

            return dtNodeInfo;
        }

        public override DataTable LoadTable()
        {            
            this.Table.BeginLoadData();
            try
            {                              
                foreach(var node in this.Cluster.Nodes)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Node State Processing for {0} Started", node.Id.NodeName());
                    int nbrItems = 0;
                    DataRow dataRow;

                    foreach (var nodeState in node.StateChanges)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow.SetField(ColumnNames.UTCTimeStamp, nodeState.EventTime.UtcDateTime);
                        dataRow.SetField(ColumnNames.LogLocalTimeStamp, nodeState.EventTimeLocal);
                        
                        dataRow.SetField(ColumnNames.DataCenter, node.DCName());
                        dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                        dataRow.SetField(Columns.SortOrder, nodeState.SortOrder());
                        dataRow.SetField(Columns.State, nodeState.State);
                        if (nodeState.Duration.HasValue)
                            dataRow.SetField(Columns.Duration, nodeState.Duration.Value);

                        if(nodeState.DetectedByNode != null)
                        {
                            dataRow.SetField(Columns.SourceNode,
                                                string.Format("{0}|{1}",
                                                                nodeState.DetectedByNode.DCName(),
                                                                nodeState.DetectedByNode.Id.NodeName()));
                        }

                        this.Table.Rows.Add(dataRow);
                        nbrItems++;
                    }

                    Logger.Instance.InfoFormat("Node State Processing for {0} completed with {1:###,###,##0}", node.Id.NodeName(), nbrItems);
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Node State Canceled");
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
