using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{
    public sealed class TaggedDetailDataTable : DataTableLoad
    {
        public TaggedDetailDataTable(DSEDiagnosticLibrary.Cluster cluster,
                                            DataTable taggedItemsDataTable,
                                            CancellationTokenSource cancellationSource = null,
                                            Guid? sessionId = null)
            : base(cluster, cancellationSource, taggedItemsDataTable, sessionId)
        {            
        }

        public override DataTable CreateInitializationTable()
        {
            var taggedDt = new DataTable(TableNames.TaggedTablesDetail, this.SourceTable.Namespace);

            foreach(DataColumn column in this.SourceTable.Columns)
            {
                var cloneCol = new DataColumn(column.ColumnName, column.DataType);
                cloneCol.AllowDBNull = column.AllowDBNull;

                taggedDt.Columns.Add(cloneCol);
            }

            return taggedDt;
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
                Logger.Instance.InfoFormat("Loading TaggedTablesDetail");

                this.CancellationToken.ThrowIfCancellationRequested();

                var rows = this.SourceTable.AsEnumerable()
                               .Where(dataRow => !dataRow.IsNull(ColumnNames.NodeIPAddress))
                               .OrderBy(dataRow => dataRow.Field<string>(ColumnNames.KeySpace))
                               .ThenBy(dataRow => dataRow.Field<string>(ColumnNames.Table))
                               .ThenBy(dataRow => dataRow.Field<string>(ColumnNames.DataCenter))
                               .ThenBy(dataRow => dataRow.Field<string>(ColumnNames.NodeIPAddress));
                
                foreach (var row in rows.ToArray())
                {
                    this.Table.Rows.Add(row.ItemArray);
                    ++nbrItems;
                }

                Logger.Instance.InfoFormat("Loaded TaggedTablesDetail, Total Nbr Items {0:###,###,##0}", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading TaggedTablesDetail Canceled");
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
