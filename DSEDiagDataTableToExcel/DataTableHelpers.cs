using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using System.Text.RegularExpressions;

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
        
        static readonly char[] Letters = new char[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="colOrdinal">zero position </param>
        /// <returns></returns>
        public static string ExcelTranslateColumnFromColOrdinaltoLetter(int colOrdinal)
        {
            var level = colOrdinal / 26;
            string colLetter;

            if (level == 0)
            {
                colLetter = Letters[colOrdinal].ToString();
            }
            else
            {
                colLetter = Letters[level - 1].ToString();
                colLetter += Letters[colOrdinal - (level * 26)].ToString();
            }

            return colLetter;
        }

        private static readonly System.Text.RegularExpressions.Regex FormulaDataColumnMatch = new Regex(Properties.Settings.Default.TranslateFormula,
                                                                                                                                            RegexOptions.IgnoreCase 
                                                                                                                                            | RegexOptions.Compiled);

        public static string TranslateFormula(DataTable dataTable, string formulaString, bool useR1C1Syntax = false)
        {
            if (string.IsNullOrEmpty(formulaString)) return formulaString;

            var regExMatches = FormulaDataColumnMatch.Matches(formulaString);

            foreach (Match element in regExMatches)
            {
                var grpDCNameInd = element.Groups["DCNameInd"];
                var grpDCName = element.Groups["DCName"];

                if (grpDCName.Success && !string.IsNullOrEmpty(grpDCName.Value))
                {
                    var dataColumn = dataTable.Columns[grpDCName.Value.Trim()];
                    formulaString = formulaString.Replace(grpDCNameInd.Value,
                                                            useR1C1Syntax
                                                            ? (dataColumn.Ordinal+1).ToString()
                                                            : ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal));
                }
            }

            return formulaString;
        }

        public static DataColumn GetColumn(this DataTable dataTable, string columnName)
        {
            return dataTable.Columns[columnName];
        }

        public static string GetExcelColumnLetter(this DataColumn dataColumn)
        {
            return ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal);
        }

        public static DataColumn SetCaption(this DataColumn dataColumn, string caption)
        {
            dataColumn.Caption = caption;
            return dataColumn;
        }

        /// <summary>
        /// Sets Column Width. If negative, a columns width attribute is removed.
        /// </summary>
        /// <param name="dataColumn"></param>
        /// <param name="colWidth"></param>
        /// <returns></returns>
        public static DataColumn SetWidth(this DataColumn dataColumn, int colWidth)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("ColumnWidth"))
            {
                if (colWidth < 0)
                    dataColumn.ExtendedProperties.Remove("ColumnWidth");
                else
                    dataColumn.ExtendedProperties["ColumnWidth"] = colWidth;
            }
            else
                dataColumn.ExtendedProperties.Add("ColumnWidth", colWidth);

            return dataColumn;
        }

        /// <summary>
        /// Sets a formula using a R1C1 content. Placeholders can be uses where:
        ///     &apos;{0}&apos; is the first row in the worksheet
        ///     &apos;{1}&apos; is the last row in the worksheet
        ///     &apos;{2}&apos; is this data column&apos;s position in the worksheet.
        ///     
        /// Note to define a data column name paceholder, surrond the data column name with &quot;{}&quot; e.g., {MyDataColumn}
        /// <param name="dataColumn"></param>
        /// <param name="r1c1Formula"></param>
        /// <returns></returns>
        /// <example>
        /// dtDCInfo.Columns.Add(&quot;Distribution Storage From&quot;, typeof(decimal)).SetFormulaR1C1(&quot;=sum(R{0}C{2}:R{1}C{2})&quot;);
        /// </example>
        public static DataColumn SetFormulaR1C1(this DataColumn dataColumn, string r1c1Formula)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("R1C1Formula"))
            {
                if (string.IsNullOrEmpty(r1c1Formula))
                    dataColumn.ExtendedProperties.Remove("R1C1Formula");
                else
                    dataColumn.ExtendedProperties["R1C1Formula"] = string.Format(TranslateFormula(dataColumn.Table, r1c1Formula, true), "{0}", "{1}", dataColumn.Ordinal + 1);
            }
            else
                dataColumn.ExtendedProperties.Add("R1C1Formula", string.Format(TranslateFormula(dataColumn.Table, r1c1Formula, true), "{0}", "{1}", dataColumn.Ordinal + 1));

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
        /// Sets a formula using an A1 content. Placeholders can be uses where:
        ///     &apos;{0}&apos; is the first row in the worksheet
        ///     &apos;{1}&apos; is the last row in the worksheet,
        ///     &apos;{2}&apos; is this data column&apos;s position in the worksheet.
        ///     
        /// Note to define a data column name paceholder, surrond the data column name with &quot;{}&quot; e.g., {MyDataColumn}
        /// <param name="dataColumn"></param>
        /// <param name="formula"></param>
        /// <returns></returns>
        /// <example>
        /// dtDCInfo.Columns.Add(&quot;Distribution Storage From&quot;, typeof(decimal)).SetFormula(&quot;=sum(A{0}:A{1})&quot;);        
        /// </example>
        public static DataColumn SetFormula(this DataColumn dataColumn, string formula)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("Formula"))
            {
                if (string.IsNullOrEmpty(formula))
                    dataColumn.ExtendedProperties.Remove("Formula");
                else
                    dataColumn.ExtendedProperties["Formula"] = string.Format(TranslateFormula(dataColumn.Table, formula),
                                                                                "{0}", "{1}",
                                                                                ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal));
            }
            else
                dataColumn.ExtendedProperties.Add("Formula", string.Format(TranslateFormula(dataColumn.Table, formula),
                                                                                "{0}", "{1}",
                                                                                ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal)));

            return dataColumn;
        }

        public static DataColumn SetDBNull(this DataColumn dataColumn, bool allowDBNull = true)
        {
            dataColumn.AllowDBNull = allowDBNull;
            return dataColumn;
        }

        public static DataColumn TotalColumn(this DataColumn dataColumn, bool haveSumTotalLine = true, bool includeCondFmt = true, bool remove = false)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("TotalColumn"))
            {
                if (remove)
                    dataColumn.ExtendedProperties.Remove("TotalColumn");
                else
                    dataColumn.ExtendedProperties["TotalColumn"] = new Tuple<bool,bool>(haveSumTotalLine,includeCondFmt);
            }
            else if (!remove)
                dataColumn.ExtendedProperties.Add("TotalColumn", new Tuple<bool,bool>(haveSumTotalLine,includeCondFmt));

            return dataColumn;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataColumn"></param>
        /// <param name="haveTotalLine"></param>
        /// <param name="includeCondFmt">
        /// Excel numeric format string. Null to use default, empty string for no format.
        /// </param>
        /// <param name="remove"></param>
        /// <returns></returns>
        public static DataColumn CountNonBlankColumn(this DataColumn dataColumn, bool haveTotalLine = true, string includeCondFmt = null, bool remove = false)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("CountNonBlankColumn"))
            {
                if (remove)
                    dataColumn.ExtendedProperties.Remove("CountNonBlankColumn");
                else
                    dataColumn.ExtendedProperties["CountNonBlankColumn"] = new Tuple<bool, string>(haveTotalLine, includeCondFmt);
            }
            else if (!remove)
                dataColumn.ExtendedProperties.Add("CountNonBlankColumn", new Tuple<bool, string>(haveTotalLine, includeCondFmt));

            return dataColumn;
        }

        public static DataColumn AvgerageColumn(this DataColumn dataColumn, bool haveAvgTotalLine = true, bool includeCondFmt = true, bool remove = false)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("AverageColumn"))
            {
                if (remove)
                    dataColumn.ExtendedProperties.Remove("AverageColumn");
                else
                    dataColumn.ExtendedProperties["AverageColumn"] = new Tuple<bool,bool>(haveAvgTotalLine,includeCondFmt);
            }
            else if (!remove)
                dataColumn.ExtendedProperties.Add("AverageColumn", new Tuple<bool,bool>(haveAvgTotalLine,includeCondFmt));

            return dataColumn;
        }

        /// <summary>
        /// Sets a formula using an A1 content. Placeholders can be uses where:
        ///     &apos;{0}&apos; is the first row in the worksheet,
        ///     &apos;{1}&apos; is the last row in the worksheet,
        ///     &apos;{2}&apos; is this data column&apos;s position in the worksheet
        ///     &apos;{3}&apos; is this data column&apos;s total row in the worksheet
        ///     
        /// Note to define a data column name paceholder, surrond the data column name with &quot;{}&quot; e.g., {MyDataColumn}
        /// </summary>
        /// <param name="dataColumn"></param>
        /// <param name="formula"></param>
        /// <param name="haveTotalLine"></param>
        /// <param name="remove"></param>
        /// <returns></returns>
        /// <example>
        /// dtDCInfo.Columns.Add(&quot;Distribution Storage From&quot;, typeof(decimal)).TotalFormulaColumn(&quot;=sum(A{0}:A{1})&quot;);        
        /// </example>
        public static DataColumn TotalFormulaColumn(this DataColumn dataColumn, string formula, bool haveTotalLine = true, bool includeCondFmt = true, bool remove = false)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("TotalFormulaColumn"))
            {
                if (remove)
                    dataColumn.ExtendedProperties.Remove("TotalFormulaColumn");
                else
                    dataColumn.ExtendedProperties["TotalFormulaColumn"] = new Tuple<string, bool, bool>(string.Format(TranslateFormula(dataColumn.Table, formula),
                                                                                                                "{0}", "{1}",
                                                                                                                ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal),
                                                                                                                "{2}"), haveTotalLine, includeCondFmt);
            }
            else if(!remove)
                dataColumn.ExtendedProperties.Add("TotalFormulaColumn", new Tuple<string, bool, bool>(string.Format(TranslateFormula(dataColumn.Table, formula),
                                                                                                                "{0}", "{1}",
                                                                                                                ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal),
                                                                                                                "{2}"), haveTotalLine, includeCondFmt));
            return dataColumn;
        }

        /// <summary>
        /// If formula is used the column letter and row number format be used. Also the following applies:
        /// Sets a formula using an A1 content. Placeholders can be uses where &apos;{0}&apos; is the first row in the worksheet, &apos;{1}&apos; is the last row in the worksheet, and &apos;{2}&apos; is this data column&apos;s position in the worksheet.
        /// </summary>
        /// <param name="dataColumn"></param>
        /// <param name="conditionalFormatValues"></param>
        /// <returns></returns>
        public static DataColumn SetConditionalFormat(this DataColumn dataColumn, params ConditionalFormatValue[] conditionalFormatValues)
        {
            if (conditionalFormatValues == null || conditionalFormatValues.Length == 0) return dataColumn;

            bool alreadyExists;

            if (alreadyExists = dataColumn.ExtendedProperties.ContainsKey("ConditionalFormat"))
            {
                if (conditionalFormatValues == null || conditionalFormatValues.IsEmpty())
                {
                    dataColumn.ExtendedProperties.Remove("ConditionalFormat");
                    return dataColumn;
                }
            }

            foreach (var conditionalFormatValue in conditionalFormatValues)
            {                
                if(conditionalFormatValue.Type == ConditionalFormatValue.Types.Formula
                    && !string.IsNullOrEmpty(conditionalFormatValue.FormulaText))
                {
                    string strFormula = null;

                    try
                    {
                        strFormula = TranslateFormula(dataColumn.Table, conditionalFormatValue.FormulaText);
                        conditionalFormatValue.FormulaText = string.Format(strFormula,
                                                                            "{0}", "{1}",
                                                                            ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal));
                    }
                    catch (System.Exception ex)
                    {
                        throw new ArgumentException(string.Format("Conditional Format Formula Error. Formula: \"{0}\" CondFmt: \"{1}\"",
                                                                    strFormula,
                                                                    conditionalFormatValue), ex);                       
                    }                    
                }

                if (conditionalFormatValue.Type == ConditionalFormatValue.Types.Formula
                    && !string.IsNullOrEmpty(conditionalFormatValue.FormulaTextBetween))
                {
                    string strFormula = null;

                    try
                    {
                        strFormula = TranslateFormula(dataColumn.Table, conditionalFormatValue.FormulaTextBetween);
                        conditionalFormatValue.FormulaTextBetween = string.Format(strFormula,
                                                                                    "{0}", "{1}",
                                                                                    ExcelTranslateColumnFromColOrdinaltoLetter(dataColumn.Ordinal));
                    }
                    catch (System.Exception ex)
                    {
                        throw new ArgumentException(string.Format("Conditional Format Between Formula Error. Formula: \"{0}\" CondFmt: \"{1}\"",
                                                                    strFormula,
                                                                    conditionalFormatValue), ex);
                    }                    
                }
            }

            if (alreadyExists)
                dataColumn.ExtendedProperties["ConditionalFormat"] = conditionalFormatValues;            
            else
                dataColumn.ExtendedProperties.Add("ConditionalFormat", conditionalFormatValues);

            return dataColumn;
        }

        public static DataColumn SetConditionalFormat(this DataColumn dataColumn, string jsonConditionalFormatValues)
        {
            if (string.IsNullOrEmpty(jsonConditionalFormatValues)) return dataColumn;
                
            var condFmtValues = Newtonsoft.Json.JsonConvert.DeserializeObject<ConditionalFormatValue[]>(jsonConditionalFormatValues);

            return SetConditionalFormat(dataColumn, condFmtValues);
        }

        public static DataColumn SetComment(this DataColumn dataColumn, string comment)
        {            
            if (dataColumn.ExtendedProperties.ContainsKey("Comment"))
            {
                if (string.IsNullOrEmpty(comment))
                    dataColumn.ExtendedProperties.Remove("Comment");
                else
                    dataColumn.ExtendedProperties["Comment"] = comment;
            }
            else if(!string.IsNullOrEmpty(comment))
                dataColumn.ExtendedProperties.Add("Comment", comment);

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
                bool hasTotalCol = false;

                if (dataColumn.ExtendedProperties.ContainsKey("TotalColumn"))
                {
                    var addTotLine = (Tuple<bool,bool>)dataColumn.ExtendedProperties["TotalColumn"];

                    workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1]
                                .FormulaR1C1 = string.Format("=sum(R{0}C{1}:R{2}C{1})", wsHeaderRow + 1, dataColumn.Ordinal + 1, currentLastRow);

                    if (addTotLine.Item1)
                    {
                        workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Double;
                    }
                    hasTotalCol = addTotLine.Item2;
                    ++currentLastRow;
                }

                if (dataColumn.ExtendedProperties.ContainsKey("CountNonBlankColumn"))
                {
                    var addCntLine = (Tuple<bool, string>)dataColumn.ExtendedProperties["CountNonBlankColumn"];

                    workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1]
                                .FormulaR1C1 = string.Format("=COUNTA(R{0}C{1}:R{2}C{1})", wsHeaderRow + 1, dataColumn.Ordinal + 1, currentLastRow);

                    if (addCntLine.Item1)
                    {
                        workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Double;
                    }
                    if (addCntLine.Item2 == null)
                        workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1].Style.Numberformat.Format = "#,###,##0";
                    else                        
                        workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1].Style.Numberformat.Format = addCntLine.Item2 == string.Empty ? null : addCntLine.Item2;

                    ++currentLastRow;
                }
                
                if (dataColumn.ExtendedProperties.ContainsKey("AverageColumn"))
                {
                    var addTotLine = (Tuple<bool,bool>)dataColumn.ExtendedProperties["AverageColumn"];

                    workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1]
                                .FormulaR1C1 = string.Format("=AVERAGE(R{0}C{1}:R{2}C{1})", wsHeaderRow + 1, dataColumn.Ordinal + 1, currentLastRow);

                    if (addTotLine.Item1)
                    {
                        workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                    }
                    hasTotalCol = addTotLine.Item2;
                    ++currentLastRow;
                }

                if (dataColumn.ExtendedProperties.ContainsKey("TotalFormulaColumn"))
                {
                    var formulaTotLine = (Tuple<string,bool,bool>) dataColumn.ExtendedProperties["TotalFormulaColumn"];

                    workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1]
                                .Formula = string.Format(formulaTotLine.Item1, wsHeaderRow + 1, currentLastRow, currentLastRow + 1);

                    if (formulaTotLine.Item2)
                    {
                        workSheet.Cells[currentLastRow + 1, dataColumn.Ordinal + 1].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                    }
                    hasTotalCol = formulaTotLine.Item3;
                    ++currentLastRow;
                }

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
                        else if (key == "ConditionalFormat")
                        {
                            var conditionalFormats = (ConditionalFormatValue[])dataColumn.ExtendedProperties["ConditionalFormat"];
                            var cndLastRow = lastRow;

                            foreach (var conditionalFormatValue in conditionalFormats)
                            {
                                if (conditionalFormatValue.Type == ConditionalFormatValue.Types.Formula
                                    && !string.IsNullOrEmpty(conditionalFormatValue.FormulaText))
                                {
                                    conditionalFormatValue.FormulaText = string.Format(conditionalFormatValue.FormulaText,
                                                                                        wsHeaderRow + 1, lastRow);
                                }

                                if (conditionalFormatValue.Type == ConditionalFormatValue.Types.Formula
                                    && !string.IsNullOrEmpty(conditionalFormatValue.FormulaTextBetween))
                                {
                                    conditionalFormatValue.FormulaTextBetween = string.Format(conditionalFormatValue.FormulaTextBetween,
                                                                                        wsHeaderRow + 1, lastRow);
                                }

                                if (hasTotalCol && conditionalFormatValue.IncludeTotalRow)
                                    cndLastRow = currentLastRow;
                            }

                            workSheet.CreateConditionalFormat(workSheet.Cells[wsHeaderRow + 1, dataColumn.Ordinal + 1, cndLastRow, dataColumn.Ordinal + 1],
                                                                conditionalFormats);
                        }
                        else if (key == "ColumnWidth")
                        {                            
                            var colWidth = (int)dataColumn.ExtendedProperties["ColumnWidth"];

                            if(colWidth >= 0)
                                workSheet.Column(dataColumn.Ordinal + 1).Width = colWidth;
                        }
                        else if (key == "Comment")
                        {
                            var hdrComment = (string)dataColumn.ExtendedProperties["Comment"];

                            workSheet.Cells[wsHeaderRow, dataColumn.Ordinal + 1].AddComment(hdrComment, "RHA");
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
                
                if (dataColumn.ExtendedProperties.ContainsKey("NumericStyle"))
                {
                    workSheet.Cells[wsHeaderRow + 1, dataColumn.Ordinal + 1, currentLastRow, dataColumn.Ordinal + 1]
                                .Style.Numberformat.Format = (string)dataColumn.ExtendedProperties["NumericStyle"];
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

        static public void ApplyColumnAttribs(this OfficeOpenXml.ExcelWorksheet workSheet, DataTable dataTable)
        {
            foreach (DataColumn dataColumn in dataTable.Columns)
            {                
                if (dataColumn.ExtendedProperties.Count == 0) continue;
                
                foreach (var item in dataColumn.ExtendedProperties.Keys)
                {
                    if (item != null && item is string)
                    {
                        var key = (string)item;

                        if (key == "ColumnWidth")
                        {
                            var colWidth = (int)dataColumn.ExtendedProperties["ColumnWidth"];

                            if (colWidth >= 0)
                                workSheet.Column(dataColumn.Ordinal + 1).Width = colWidth;
                        }                        
                    }
                }                
            }
        }

        static public void AltFileFillRow(this OfficeOpenXml.ExcelWorksheet workSheet,
                                            int startRow,
                                            DataColumn changeRowTest,
                                            DataColumn changeRowTest1 = null,
                                            DataColumn partalRowTest = null,
                                            DataColumn emptyRowFromCol = null,
                                            DataColumn emptyRowToCol = null)
        {
            int? endRowTest = workSheet.Dimension?.End?.Row;

            if (!endRowTest.HasValue || endRowTest.Value < startRow) return;

            var endRow = endRowTest.Value;

            string lastValue = string.Empty;
            string currentValue;
            bool formatOn = false;
            int testCol = changeRowTest.Ordinal + 1;
            int testCol1 = changeRowTest1?.Ordinal + 1 ?? -1;
            Tuple<int, int> partalFill = null;
            Tuple<int, int> partalEmptyFill = null;
            Tuple<int, int> partalFillBack = null;
            int partialTestCol = -1;

            if(partalRowTest != null && (emptyRowFromCol != null || emptyRowToCol != null))
            {
                var endCol = workSheet.Dimension.End.Column;
                partialTestCol = partalRowTest.Ordinal + 1;

                if(emptyRowFromCol != null && emptyRowFromCol.Ordinal > 0)
                {
                    partalFill = new Tuple<int, int>(1, emptyRowFromCol.Ordinal);
                }
                if (emptyRowToCol != null && emptyRowToCol.Ordinal + 1 < endCol)
                {
                    partalFillBack = new Tuple<int, int>(emptyRowToCol.Ordinal + 2, endCol);
                }
                partalEmptyFill = new Tuple<int, int>(emptyRowFromCol == null ? emptyRowToCol.Ordinal + 1 : emptyRowFromCol.Ordinal + 1,
                                                      emptyRowToCol == null ? emptyRowFromCol.Ordinal + 1 : emptyRowToCol.Ordinal + 1);
            }
            
            for (int nRow = startRow; nRow <= endRow; ++nRow)
            {
                if (workSheet.Cells[nRow, testCol]?.Value != null)
                {
                    if (testCol1 > 0)
                        currentValue = string.Format("{0}|{1}", workSheet.Cells[nRow, testCol].Value, workSheet.Cells[nRow, testCol1]?.Value);
                    else
                        currentValue = workSheet.Cells[nRow, testCol].Value.ToString();

                    if (lastValue == null)
                    {
                        lastValue = currentValue;
                        formatOn = false;
                    }
                    else if (lastValue != currentValue)
                    {
                        lastValue = currentValue;
                        formatOn = !formatOn;
                    }

                    workSheet.Row(nRow).Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.None;
                    if (formatOn)
                    {                        
                        if (partialTestCol > 0 && workSheet.Cells[nRow, partialTestCol].Value == null)
                        {
                            //workSheet.Cells[string.Format("A{0}:D{0}", nRow)].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                            if (partalFill != null)
                            {
                                workSheet.Cells[nRow, partalFill.Item1, nRow, partalFill.Item2].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                                workSheet.Cells[nRow, partalFill.Item1, nRow, partalFill.Item2].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                            }
                            if (partalFillBack != null)
                            {
                                workSheet.Cells[nRow, partalFillBack.Item1, nRow, partalFillBack.Item2].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                                workSheet.Cells[nRow, partalFillBack.Item1, nRow, partalFillBack.Item2].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                            }

                            workSheet.Cells[nRow, partalEmptyFill.Item1, nRow, partalEmptyFill.Item2].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.DarkGrid;
                            workSheet.Cells[nRow, partalEmptyFill.Item1, nRow, partalEmptyFill.Item2].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.DarkGray);
                        }
                        else
                        {
                            workSheet.Row(nRow).Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                            workSheet.Row(nRow).Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                        }
                    }
                    else
                    {
                        if (partialTestCol > 0 && workSheet.Cells[nRow, partialTestCol].Value == null)
                        {
                            //workSheet.Cells[string.Format("A{0}:D{0}", nRow)].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;                            
                            workSheet.Cells[nRow, partalEmptyFill.Item1, nRow, partalEmptyFill.Item2].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGrid;
                            workSheet.Cells[nRow, partalEmptyFill.Item1, nRow, partalEmptyFill.Item2].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.WhiteSmoke);
                        }                         
                    }                    
                }
            }            
        }
    }
}
