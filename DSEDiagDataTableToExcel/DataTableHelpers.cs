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
        
        static readonly char[] Leters = new char[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="colIndex">zero position </param>
        /// <returns></returns>
        public static string ExcelTranslateColumnFromColPostoLeter(int colIndex)
        {
            var level = colIndex / 26;
            string colLetter;

            if (level == 0)
            {
                colLetter = Leters[colIndex].ToString();
            }
            else
            {
                colLetter = Leters[level - 1].ToString();
                colLetter += Leters[colIndex - (level * 26)].ToString();
            }

            return colLetter;
        }
                      
        public static DataColumn GetColumn(this DataTable dataTable, string columnName)
        {
            return dataTable.Columns[columnName];
        }

        public static string GetExcelColumnLetter(this DataColumn dataColumn)
        {
            return ExcelTranslateColumnFromColPostoLeter(dataColumn.Ordinal);
        }

        public static DataColumn SetCaption(this DataColumn dataColumn, string caption)
        {
            dataColumn.Caption = caption;
            return dataColumn;
        }

        /// <summary>
        /// Sets a formula using a R1C1 content. Placeholders can be uses where &apos;{0}&apos; is the first row in the worksheet, &apos;{1}&apos; is the last row in the worksheet, and &apos;{2}&apos; is this data column&apos;s position in the worksheet.
        /// <param name="dataColumn"></param>
        /// <param name="r1c1Formula"></param>
        /// <returns></returns>
        /// <example>
        /// dtDCInfo.Columns.Add(&quot;Distribution Storage From&quot;, typeof(decimal)).SetFormulaR1C1(&quot;sum(R{0}C{2}:R{1}C{2})&quot;);
        /// </example>
        public static DataColumn SetFormulaR1C1(this DataColumn dataColumn, string r1c1Formula)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("R1C1Formula"))
            {
                if (string.IsNullOrEmpty(r1c1Formula))
                    dataColumn.ExtendedProperties.Remove("R1C1Formula");
                else
                    dataColumn.ExtendedProperties["R1C1Formula"] = r1c1Formula;
            }
            else
                dataColumn.ExtendedProperties.Add("R1C1Formula", string.Format(r1c1Formula, "{0}", "{1}", dataColumn.Ordinal + 1));

            return dataColumn;
        }

        public static DataColumn SetNumericFormat(this DataColumn dataColumn, string numericFormat)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("NumericStyle"))
            {
                if (string.IsNullOrEmpty(numericFormat))
                    dataColumn.ExtendedProperties.Remove("NumericStyle");
                else
                    dataColumn.ExtendedProperties["NumericStyle"] = numericFormat;
            }
            else
                dataColumn.ExtendedProperties.Add("NumericStyle", numericFormat);

            return dataColumn;
        }

        public static DataColumn SetHeaderText(this DataColumn dataColumn, string headerText, int rowOffset, bool defineBoarders = false)
        {
            var key = string.Format("HeaderText-{0}", rowOffset);

            if (dataColumn.ExtendedProperties.ContainsKey(key))
            {
                if (string.IsNullOrEmpty(headerText))
                    dataColumn.ExtendedProperties.Remove(key);
                else
                    dataColumn.ExtendedProperties[key] = new Tuple<string, int, bool>(headerText, rowOffset, defineBoarders);
            }
            else
                dataColumn.ExtendedProperties.Add(key, new Tuple<string, int, bool>(headerText, rowOffset, defineBoarders));

            return dataColumn;
        }

        public static IEnumerable<DataColumn> SetGroupHeader(this DataTable dataTable, string headerText, int rowOffset, bool defineBoarders, params DataColumn[] dataColumns)
        {
            if (dataColumns.Length == 0) return dataColumns;
            if (dataColumns.Length == 1)
            {
                SetHeaderText(dataColumns[0], headerText, rowOffset, defineBoarders);
                return dataColumns;
            }

            var colPositions = dataColumns.OrderBy(c => c.Ordinal);
            var key = string.Format("GroupHeadetText-{0}-{1}-{2}",
                                    colPositions.First().ColumnName,
                                    colPositions.Last().ColumnName,
                                    rowOffset);

            if (dataTable.ExtendedProperties.ContainsKey(key))
            {
                if (string.IsNullOrEmpty(headerText))
                    dataTable.ExtendedProperties.Remove(key);
                else
                    dataTable.ExtendedProperties[key] = new Tuple<string, int, bool, int, int>(headerText,
                                                                                                rowOffset,
                                                                                                defineBoarders,
                                                                                                colPositions.First().Ordinal + 1,
                                                                                                colPositions.Last().Ordinal + 1);
            }
            else
                dataTable.ExtendedProperties.Add(key,
                                                    new Tuple<string, int, bool, int, int>(headerText,
                                                                                            rowOffset,
                                                                                            defineBoarders,
                                                                                            colPositions.First().Ordinal + 1,
                                                                                            colPositions.Last().Ordinal + 1));

            return dataColumns;
        }

        public static IEnumerable<DataColumn> SetGroupHeader(this DataTable dataTable, string headerText, int rowOffset, bool defineBoarders, IEnumerable<DataColumn> prevGroup, params DataColumn[] dataColumns)
        {
            return SetGroupHeader(dataTable, headerText, rowOffset, defineBoarders, prevGroup.Concat(dataColumns).ToArray());
        }

        public static IEnumerable<DataColumn> SetGroupHeader(this DataTable dataTable, string headerText, int rowOffset, bool defineBoarders, params IEnumerable<DataColumn>[] dataColumns)
        {
            return SetGroupHeader(dataTable, headerText, rowOffset, defineBoarders, dataColumns.SelectMany(i => i).ToArray());
        }

        /// <summary>
        /// Sets a formula using an A1 content. Placeholders can be uses where &apos;{0}&apos; is the first row in the worksheet, &apos;{1}&apos; is the last row in the worksheet, and &apos;{2}&apos; is this data column&apos;s position in the worksheet.
        /// <param name="dataColumn"></param>
        /// <param name="formula"></param>
        /// <returns></returns>
        /// <example>
        /// dtDCInfo.Columns.Add(&quot;Distribution Storage From&quot;, typeof(decimal)).SetFormulaR1C1(&quot;sum(R{0}C{2}:R{1}C{2})&quot;);
        /// </example>
        public static DataColumn SetFormula(this DataColumn dataColumn, string formula)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("Formula"))
            {
                if (string.IsNullOrEmpty(formula))
                    dataColumn.ExtendedProperties.Remove("Formula");
                else
                    dataColumn.ExtendedProperties["Formula"] = formula;
            }
            else
                dataColumn.ExtendedProperties.Add("Formula", string.Format(formula, "{0}", "{1}", ExcelTranslateColumnFromColPostoLeter(dataColumn.Ordinal)));

            return dataColumn;
        }

        public static DataColumn TotalColumn(this DataColumn dataColumn, bool haveSumTotalLine = true, bool remove = false)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("TotalColumn"))
            {
                if (remove)
                    dataColumn.ExtendedProperties.Remove("TotalColumn");
                else
                    dataColumn.ExtendedProperties["TotalColumn"] = haveSumTotalLine;
            }
            else
                dataColumn.ExtendedProperties.Add("TotalColumn", haveSumTotalLine);

            return dataColumn;
        }
        
        static public void UpdateWorksheet(this OfficeOpenXml.ExcelWorksheet workSheet, DataTable dataTable, int wsHeaderRow)
        {
            var lastRow = workSheet.Dimension.End.Row;

            foreach (DataColumn dataColumn in dataTable.Columns)
            {
                if (!string.IsNullOrEmpty(dataColumn.Caption))
                {
                    workSheet.Cells[wsHeaderRow, dataColumn.Ordinal + 1].Value = dataColumn.Caption;
                }

                if (dataColumn.ExtendedProperties.Count == 0) continue;
                var currentLastRow = lastRow;

                foreach (var item in dataColumn.ExtendedProperties.Keys)
                {
                    if(item != null && item is string)
                    {
                        var key = (string)item;
                        
                        if (key == "R1C1Formula")
                        {
                            var formula = (string)dataColumn.ExtendedProperties["R1C1Formula"];

                            workSheet.Cells[lastRow + 1, dataColumn.Ordinal + 1]
                                       .FormulaR1C1 = string.Format(formula, wsHeaderRow + 1, lastRow);
                        }
                        else if (key == "Formula")
                        {
                            var formula = (string)dataColumn.ExtendedProperties["Formula"];

                            workSheet.Cells[lastRow + 1, dataColumn.Ordinal + 1]
                                       .Formula = string.Format(formula, wsHeaderRow + 1, lastRow);
                        }
                        else if (key == "TotalColumn")
                        {
                            bool addTotLine = (bool)dataColumn.ExtendedProperties["TotalColumn"];
                            
                            workSheet.Cells[lastRow + 1, dataColumn.Ordinal + 1]
                                        .FormulaR1C1 = string.Format("=sum(R{0}C{1}:R{2}C{1})", wsHeaderRow + 1, dataColumn.Ordinal + 1, lastRow);

                            if (addTotLine)
                            {
                                workSheet.Cells[lastRow + 1, dataColumn.Ordinal + 1].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                            }
                            ++currentLastRow;
                        }
                        else if (key == "NumericStyle")
                        {
                            workSheet.Cells[wsHeaderRow + 1, dataColumn.Ordinal + 1, currentLastRow, dataColumn.Ordinal + 1]
                                        .Style.Numberformat.Format = (string)dataColumn.ExtendedProperties["NumericStyle"];
                        }
                        else if (key.StartsWith("HeaderText"))
                        {
                            var grpInfo = (Tuple<string,int,bool>) dataColumn.ExtendedProperties[key];
                            var targetRow = wsHeaderRow + grpInfo.Item2;

                            if (targetRow < 1) targetRow = 1;

                            workSheet.Cells[targetRow, dataColumn.Ordinal + 1].Value = grpInfo.Item1;

                            if (grpInfo.Item3)
                            {
                                workSheet.Cells[targetRow, dataColumn.Ordinal + 1, currentLastRow, dataColumn.Ordinal + 1]
                                            .Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Thin;
                                workSheet.Cells[targetRow, dataColumn.Ordinal + 1, currentLastRow, dataColumn.Ordinal + 1]
                                            .Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Thin;
                            }
                        }
                    }
                }                
            }

            if(dataTable.ExtendedProperties.Count > 0)
            {
                lastRow = workSheet.Dimension.End.Row;

                foreach (var key in dataTable.ExtendedProperties.Keys)
                {
                    if(key != null && key is string)
                    {
                        if(((string)key).StartsWith("GroupHeadetText"))
                        {
                            var value = dataTable.ExtendedProperties[key];
                            var grpInfo = (Tuple<string,int,bool,int,int>) value;

                            var targetRow = wsHeaderRow + grpInfo.Item2;
                            if (targetRow < 1) targetRow = 1;                            

                            workSheet.Cells[targetRow, grpInfo.Item4, targetRow, grpInfo.Item5].Style.WrapText = true;
                            workSheet.Cells[targetRow, grpInfo.Item4, targetRow, grpInfo.Item5].Merge = true;
                            workSheet.Cells[targetRow, grpInfo.Item4, targetRow, grpInfo.Item5].Value = grpInfo.Item1;

                            if (grpInfo.Item3)
                            {
                                workSheet.Cells[targetRow, grpInfo.Item4, lastRow, grpInfo.Item4]
                                            .Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Thin;
                                workSheet.Cells[targetRow, grpInfo.Item5, lastRow, grpInfo.Item5]
                                            .Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Thin;                                
                            }
                        }
                    }
                }
            }
        }
    }
}
