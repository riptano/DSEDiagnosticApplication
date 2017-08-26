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
    public sealed class TokenRangesDataTable : DataTableLoad
    {

        public TokenRangesDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null)
            : base(cluster, cancellationSource)
        { }

        public override DataTable CreateInitializationTable()
        {
            var dtTokenRange = new DataTable(TableNames.TokenRanges, TableNames.Namespace);

            dtTokenRange.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtTokenRange.Columns.Add(ColumnNames.NodeIPAddress, typeof(string));
            dtTokenRange.Columns.Add("Start Token (exclusive)", typeof(string));
            dtTokenRange.Columns.Add("End Token (inclusive)", typeof(string));
            dtTokenRange.Columns.Add("Slots", typeof(string));
            dtTokenRange.Columns.Add("Load(MB)", typeof(decimal)).AllowDBNull = true;
            dtTokenRange.Columns.Add("Wraps Range", typeof(bool)).AllowDBNull = true;

            dtTokenRange.DefaultView.ApplyDefaultSort = false;
            dtTokenRange.DefaultView.AllowDelete = false;
            dtTokenRange.DefaultView.AllowEdit = false;
            dtTokenRange.DefaultView.AllowNew = false;
            dtTokenRange.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC, [Start Token (exclusive)] ASC", ColumnNames.DataCenter, ColumnNames.NodeIPAddress);

            return dtTokenRange;
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

                    Logger.Instance.InfoFormat("Loading Token Ranges for DC \"{0}\"", dataCenter.Name);

                    foreach (var node in dataCenter.Nodes)
                    {
                        foreach (var tokenRange in node.DSE.TokenRanges)
                        {
                            this.CancellationToken.ThrowIfCancellationRequested();

                            dataRow = this.Table.NewRow();

                            dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);
                            dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                            dataRow.SetField("Start Token (exclusive)", tokenRange.StartRange.ToString());
                            dataRow.SetField("End Token (inclusive)", tokenRange.EndRange.ToString());
                            dataRow.SetField("Slots", tokenRange.Slots.ToString("###,###,###,###,##0"));
                            dataRow.SetFieldToDecimal("Load(MB)", tokenRange.Load, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                            if(tokenRange.WrapsRange) dataRow.SetField("Wraps Range", true);

                            this.Table.Rows.Add(dataRow);
                            ++nbrItems;
                        }
                    }

                    Logger.Instance.InfoFormat("Loaded Token Ranges for DC \"{0}\", Total Nbr Items {1:###,###,##0}", dataCenter.Name, nbrItems);
                }
            }
            catch(OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Token Ranges Canceled");
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
