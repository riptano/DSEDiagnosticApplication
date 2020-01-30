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
    public sealed class TaggedDCDataTable : DataTableLoad
    {
        public const string DataCenterColPrefix = "DataCenter";

        public struct Columns
        {
            public const string KSCSType = "Keyspace/Compaction/Type";
            public const string BaseTableFactor = "Base Table Factor";
        }

        public static DataColumn[] AddDCColumns(DataTable targetTable, DataTable sourceTable)
        {
            var dataCenters = (from dataRow in sourceTable.AsEnumerable()
                               let dataCenterName = dataRow.Field<string>(ColumnNames.DataCenter)
                               where !string.IsNullOrEmpty(dataCenterName)
                               select dataCenterName)
                               .Distinct()
                               .OrderBy(i => i);
            var dataCenterAbbr = dataCenters.Select(n => DSEDiagnosticLibrary.StringHelpers.AbbrName(n));

            return MiscHelpers.AddColumnsToTable(targetTable,
                                                    dataCenters,
                                                    typeof(decimal),
                                                    true,
                                                    dataCenterAbbr,
                                                    DataCenterColPrefix).ToArray();          
        }

        public TaggedDCDataTable(DSEDiagnosticLibrary.Cluster cluster,
                                            DataTable taggedItemsDataTable,
                                            CancellationTokenSource cancellationSource = null,
                                            Guid? sessionId = null)
            : base(cluster, cancellationSource, taggedItemsDataTable, sessionId)
        {            
        }
        
        public override DataTable CreateInitializationTable()
        {
            var taggedDt = new DataTable(TableNames.TaggedDCTables, this.SourceTable.Namespace);

            foreach (DataColumn column in this.SourceTable.Columns)
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
                Logger.Instance.InfoFormat("Loading TaggedTables");

                this.CancellationToken.ThrowIfCancellationRequested();

                var rows = this.SourceTable.AsEnumerable()
                                .Where(dataRow => dataRow.IsNull(ColumnNames.NodeIPAddress))
                                .OrderBy(dataRow => dataRow.Field<string>(ColumnNames.KeySpace))
                                .ThenBy(dataRow => dataRow.Field<string>(ColumnNames.Table))
                                .ThenBy(dataRow => dataRow.Field<string>(ColumnNames.DataCenter));
                
                foreach (var row in rows.ToArray())
                {
                    this.Table.Rows.Add(row.ItemArray);
                    ++nbrItems;
                }
                this.Table.Columns.Remove(ColumnNames.NodeIPAddress);

                Logger.Instance.InfoFormat("Loaded TaggedTables, Total Nbr Items {0:###,###,##0}", nbrItems);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading TaggedTables Canceled");
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
