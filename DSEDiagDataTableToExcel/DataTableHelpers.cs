using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;

namespace DataTableToExcel
{
    public static class DataTableHelpers
    {
        public static IEnumerable<DataTable> SplitTable(this DataTable dtComplete, int nbrRowsInSubTables, bool useDefaultViewBeforeSplit)
        {
            var dtSplits = new List<DataTable>();
            var dtCurrent = new DataTable(dtComplete.TableName + "-Split-0");
            int totalRows = 0;
            long rowNbr = 0;

            if (dtComplete.Rows.Count <= nbrRowsInSubTables)
            {
                dtSplits.Add(dtComplete);
                return dtSplits;
            }

            if (useDefaultViewBeforeSplit)
            {
                if (string.IsNullOrEmpty(dtComplete.DefaultView.Sort)
                         && string.IsNullOrEmpty(dtComplete.DefaultView.RowFilter)
                         && dtComplete.DefaultView.RowStateFilter == DataViewRowState.CurrentRows)
                {
                    useDefaultViewBeforeSplit = false;
                }
                else
                {
                    dtComplete = dtComplete.DefaultView.ToTable();
                }
            }

            dtComplete
                .Columns
                .Cast<DataColumn>()
                .ForEach(dc => dtCurrent.Columns.Add(dc.ColumnName, dc.DataType).AllowDBNull = dc.AllowDBNull);

            dtCurrent.BeginLoadData();

            foreach (DataRow drSource in dtComplete.Rows)
            {
                if (totalRows > nbrRowsInSubTables)
                {
                    dtCurrent.EndLoadData();
                    dtSplits.Add(dtCurrent);
                    dtCurrent = new DataTable(dtComplete.TableName + "-Split-" + rowNbr);
                    dtComplete
                        .Columns
                        .Cast<DataColumn>()
                        .ForEach(dc => dtCurrent.Columns.Add(dc.ColumnName, dc.DataType).AllowDBNull = dc.AllowDBNull);
                    dtCurrent.BeginLoadData();
                    totalRows = 0;
                }

                dtCurrent.LoadDataRow(drSource.ItemArray, LoadOption.OverwriteChanges);
                ++totalRows;
                ++rowNbr;
            }

            dtCurrent.EndLoadData();
            dtSplits.Add(dtCurrent);

            return dtSplits;
        }
    }
}
