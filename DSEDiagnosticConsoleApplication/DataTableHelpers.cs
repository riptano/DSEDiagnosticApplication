using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticConsoleApplication
{
    public static class DataTableHelpers
    {
        public static DataTable MergeIntoOneDataTable(this Common.Patterns.Collections.LockFree.Stack<DataTable> multipleDataTables,
                                                        Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null)
        {
            DataTable dtComplete = new DataTable();
            DataTable dtItem = null;
            DataRow[] dtErrors;
            bool firstDT = true;
            string firstTableName = string.Empty;
            string lastTableName = string.Empty;
            long rowCount = 0;
            int nbrMergedTables = 0;

            dtComplete.BeginLoadData();

            while (multipleDataTables.Pop(out dtItem))
            {
                dtItem.AcceptChanges();
                ++nbrMergedTables;

                if (firstDT)
                {
                    dtItem
                        .Columns
                        .Cast<DataColumn>()
                        .ForEach(dc => dtComplete.Columns.Add(dc.ColumnName, dc.DataType).AllowDBNull = dc.AllowDBNull);
                    firstDT = dtComplete.Columns.Count == 0;
                    firstTableName = dtItem.TableName;
                }

                lastTableName = dtItem.TableName;

                if (dtItem.Rows.Count == 0) continue;

                rowCount += dtItem.Rows.Count;
                dtErrors = dtItem.GetErrors();
                if (dtErrors.Length > 0)
                {
                    dtErrors.Dump(Logger.DumpType.Error, "Table \"{0}\" Has Error", dtItem.TableName);
                    Program.ConsoleErrors.Increment("Data Table has Errors");
                }

                using (var dtReader = dtItem.CreateDataReader())
                {
                    dtComplete.Load(dtReader);
                }

                dtItem.Clear();
            }

            dtComplete.EndLoadData();
            dtComplete.AcceptChanges();

            dtComplete.TableName = CommonChars(firstTableName, lastTableName) + "-AllRows";

            if (dtComplete.Rows.Count != rowCount)
            {
                Logger.Instance.ErrorFormat("Row Counts do not match when merging from {0} tables into one table. First/Last tables are: \"{1}\"/\"{2}\". Total Rows: {3} Merged Rows: {4}",
                                                nbrMergedTables,
                                                firstTableName,
                                                lastTableName,
                                                rowCount,
                                                dtComplete.Rows.Count);
            }

            if (dtComplete.Rows.Count == 0)
            {
                if (viewFilterSortRowStateOpts != null)
                {
                    if (dtComplete.Columns.Count == 0 && firstDT && dtItem != null)
                    {
                        dtItem
                            .Columns
                            .Cast<DataColumn>()
                            .ForEach(dc => dtComplete.Columns.Add(dc.ColumnName, dc.DataType).AllowDBNull = dc.AllowDBNull);
                    }

                    if (dtComplete.Columns.Count > 0)
                    {
                        dtComplete.DefaultView.RowFilter = viewFilterSortRowStateOpts.Item1;
                        dtComplete.DefaultView.Sort = viewFilterSortRowStateOpts.Item2;
                        dtComplete.DefaultView.RowStateFilter = viewFilterSortRowStateOpts.Item3;

                        dtComplete.TableName = dtComplete.TableName
                                            + (string.IsNullOrEmpty(viewFilterSortRowStateOpts.Item1) ? string.Empty : "-Filtered")
                                            + (string.IsNullOrEmpty(viewFilterSortRowStateOpts.Item2) ? string.Empty : "-Sorted")
                                            + (viewFilterSortRowStateOpts.Item3 == DataViewRowState.None ? string.Empty : "-RowState{" + viewFilterSortRowStateOpts.Item3.ToString() + "}");
                    }
                }

                return dtComplete;
            }

            if (viewFilterSortRowStateOpts != null)
            {
                var tableName = dtComplete.TableName;
                var defaultView = new DataView(dtComplete,
                                                viewFilterSortRowStateOpts.Item1,
                                                viewFilterSortRowStateOpts.Item2,
                                                viewFilterSortRowStateOpts.Item3);
                dtComplete = defaultView.ToTable();
                dtComplete.TableName = tableName
                                            + (string.IsNullOrEmpty(viewFilterSortRowStateOpts.Item1) ? string.Empty : "-Filtered")
                                            + (string.IsNullOrEmpty(viewFilterSortRowStateOpts.Item2) ? string.Empty : "-Sorted")
                                            + (viewFilterSortRowStateOpts.Item3 == DataViewRowState.None ? string.Empty : "-RowState{" + viewFilterSortRowStateOpts.Item3.ToString() + "}");

                dtComplete.DefaultView.RowFilter = viewFilterSortRowStateOpts.Item1;
                dtComplete.DefaultView.Sort = viewFilterSortRowStateOpts.Item2;
                dtComplete.DefaultView.RowStateFilter = viewFilterSortRowStateOpts.Item3;
            }

            return dtComplete;
        }

        static string CommonChars(string a, string b)
        {
            if (a == b)
            {
                return a;
            }
            if (string.IsNullOrEmpty(a))
            {
                return b;
            }
            if (string.IsNullOrEmpty(b))
            {
                return a;
            }

            var newStr = new StringBuilder();
            var aArray = a.ToCharArray();
            var bArray = b.ToCharArray();
            int aPosSave = 0;
            int bPosSave = 0;
            bool stop = false;
            int trying = 0;

            for (int naPos = 0, nbPos = 0; !stop && naPos < a.Length && nbPos < b.Length;)
            {
                if (aArray[naPos] == bArray[nbPos])
                {
                    newStr.Append(aArray[naPos]);
                    aPosSave = ++naPos;
                    bPosSave = ++nbPos;
                    trying = 0;
                }
                else if (trying == 0 || trying == 2)
                {
                    ++naPos;
                    if (naPos >= a.Length)
                    {
                        if (trying == 0)
                        {
                            trying = 1;
                            naPos = aPosSave;
                        }
                        else
                        {
                            stop = true;
                        }
                    }

                }
                else if (trying == 1)
                {
                    ++nbPos;
                    if (nbPos >= b.Length)
                    {
                        trying = 2;
                        nbPos = bPosSave;
                    }
                }
            }

            return newStr.ToString();
        }

        public static IEnumerable<DataTable> SplitTable(this DataTable dtComplete, int nbrRowsInSubTables)
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
