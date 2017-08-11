using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using OfficeOpenXml;

namespace DSEDiagnosticConsoleApplication
{
    static public partial class DTLoadIntoExcel
    {
        static public void AutoFitColumn(ExcelWorksheet workSheet, params ExcelRange[] autoFitRanges)
        {
            try
            {
                if (autoFitRanges == null || autoFitRanges.Length == 0)
                {
                    workSheet.Cells.AutoFitColumns();
                }
                else
                {
                    foreach (var range in autoFitRanges)
                    {
                        range.AutoFitColumns();
                    }
                }
            }
            catch (System.Exception ex)
            {
                Logger.Instance.Error(string.Format("Excel Cell Range AutoFitColumns Exception Occurred in Worksheet \"{0}\". Worksheet was loaded/updated but NOT auto-formatted...", workSheet.Name), ex);
            }
        }

        static public void AutoFitColumn(ExcelWorksheet workSheet)
        {
            try
            {
                workSheet.Cells.AutoFitColumns();
            }
            catch (System.Exception ex)
            {
                Logger.Instance.Error(string.Format("Excel AutoFitColumns Exception Occurred in Worksheet \"{0}\". Worksheet was loaded/updated but NOT auto-formatted...", workSheet.Name), ex);
            }
        }

        static public ExcelRangeBase WorkSheet(ExcelPackage excelPkg,
                                                string workSheetName,
                                                System.Data.DataTable dtExcel,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                string startingWSCell = "A1",
                                                bool divideWorksheetIfExceedMaxRows = true)
        {
            Program.ConsoleExcel.Increment(string.Format("{0} - {1}", workSheetName, dtExcel.TableName));

            dtExcel.AcceptChanges();

            var dtErrors = dtExcel.GetErrors();
            if (dtErrors.Length > 0)
            {
                dtErrors.Dump(Logger.DumpType.Error, "Table \"{0}\" Has Error", dtExcel.TableName);
                Program.ConsoleErrors.Increment("Data Table has Errors");
            }

            if (dtExcel.Rows.Count == 0) return null;

            if (viewFilterSortRowStateOpts != null)
            {
                dtExcel = (new DataView(dtExcel,
                                        viewFilterSortRowStateOpts.Item1,
                                        viewFilterSortRowStateOpts.Item2,
                                        viewFilterSortRowStateOpts.Item3))
                                .ToTable();
            }

            if (dtExcel.Rows.Count == 0) return null;

            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            if (workSheet == null)
            {
                try
                {
                    workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
                }
                catch (InvalidOperationException)
                {
                    workSheet = excelPkg.Workbook.Worksheets[workSheetName];

                    if (workSheet == null)
                    {
                        throw;
                    }
                }
                catch
                {
                    throw;
                }
            }
            else
            {
                workSheet.Cells.Clear();
                foreach (ExcelComment comment in workSheet.Comments.Cast<ExcelComment>().ToArray())
                {
                    try
                    {
                        workSheet.Comments.Remove(comment);
                    }
                    catch { }
                }
            }

            if (viewFilterSortRowStateOpts == null || string.IsNullOrEmpty(viewFilterSortRowStateOpts.Item1))
            {
                Logger.Instance.InfoFormat("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\". Rows: {2:###,###,##0}", dtExcel.TableName, workSheet.Name, dtExcel.Rows.Count);
            }
            else
            {
                Logger.Instance.InfoFormat("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\" with Filter \"{2}\". Rows: {3:###,###,##0}",
                                                dtExcel.TableName, workSheet.Name, viewFilterSortRowStateOpts.Item1, dtExcel.Rows.Count);
            }

            if (dtExcel.Rows.Count > ExcelPackage.MaxRows)
            {
                if (divideWorksheetIfExceedMaxRows && ParserSettings.DivideWorksheetIfExceedMaxRows)
                {
                    Logger.Instance.ErrorFormat("Table \"{0}\" with {1:###,###,##0} rows exceed maximum number of rows allowed by EPPlus (Max. Limit of {2:###,###,##0}) for worksheet \"{3}\" in workbook \"{4}\". The worksheet will be divided, creating multiple worksheets with a 1,000,000 rows each. This may cause some workbook features to not work correctly!",
                                                dtExcel.TableName,
                                                dtExcel.Rows.Count,
                                                ExcelPackage.MaxRows,
                                                workSheetName,
                                                excelPkg?.File?.FullName);
                    var tempStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
                    tempStack.Push(dtExcel);

                    return WorkSheet(excelPkg,
                                        workSheetName,
                                        tempStack,
                                        worksheetAction,
                                        true,
                                        1000000,
                                        viewFilterSortRowStateOpts,
                                        startingWSCell,
                                        false);
                }
                else
                {
                    throw new ArgumentOutOfRangeException("dtExcel.Rows.Count",
                                                            string.Format("Table \"{0}\" with {1:###,###,##0} rows exceed maximum number of rows allowed by EPPlus (Max. Limit of {2:###,###,##0}) for worksheet \"{3}\" in workbook \"{4}\". Maybe seperate the clusters into different datacenters, enable DivideWorksheetIfExceedMaxRows parameter, or divide worksheet creation into smaller chucks.",
                                                                            dtExcel.TableName,
                                                                            dtExcel.Rows.Count,
                                                                            ExcelPackage.MaxRows,
                                                                            workSheetName,
                                                                            excelPkg?.File?.FullName));
                }
            }

            var loadRange = workSheet.Cells[startingWSCell].LoadFromDataTable(dtExcel, true);

            if (loadRange != null && worksheetAction != null)
            {
                try
                {
                    worksheetAction(workSheet);
                }
                catch (System.Exception ex)
                {
                    Logger.Instance.Error(string.Format("Exception Occurred in Worksheet \"{0}\" for Workbook \"{1}\"", workSheetName, excelPkg?.File?.FullName), ex);
                }
            }

            Program.ConsoleExcel.TaskEnd(string.Format("{0} - {1}", workSheetName, dtExcel.TableName));

            return loadRange;
        }

        static public ExcelRangeBase WorkSheet(ExcelPackage excelPkg,
                                                string workSheetName,
                                                Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable> dtExcels,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                bool enableMaxRowLimitPerWorkSheet = true,
                                                int maxRowInExcelWorkSheet = -1,
                                                Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                string startingWSCell = "A1",
                                                bool renameFirstWorksheetIfDivided = true)
        {
            DataTable dtComplete = dtExcels.MergeIntoOneDataTable(viewFilterSortRowStateOpts);

            if (dtComplete.Rows.Count == 0) return null;

            if (enableMaxRowLimitPerWorkSheet
                        && maxRowInExcelWorkSheet > 0
                        && dtComplete.Rows.Count > maxRowInExcelWorkSheet)
            {
                var dtSplits = dtComplete.SplitTable(maxRowInExcelWorkSheet);
                ExcelRangeBase excelRange = null;
                int splitCnt = 1;

                foreach (var dtSplit in dtSplits)
                {
                    excelRange = WorkSheet(excelPkg,
                                            renameFirstWorksheetIfDivided || splitCnt > 1
                                                ? string.Format("{0}-{1:000}", workSheetName, splitCnt)
                                                : workSheetName,
                                            dtSplit,
                                            worksheetAction,
                                            null,
                                            startingWSCell,
                                            false);
                    ++splitCnt;
                }

                return excelRange;
            }

            return WorkSheet(excelPkg,
                            workSheetName,
                            dtComplete,
                            worksheetAction,
                            null,
                            startingWSCell,
                            false);
        }

        static public void WorkSheetLoadColumnDefaults(ExcelWorksheet workSheet,
                                                        string column,
                                                        string[] defaultValues)
        {
            for (int emptyRow = workSheet.Dimension.End.Row + 1, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
            }

        }

        static public void WorkSheetLoadColumnDefaults(ExcelPackage excelPkg,
                                                        string workSheetName,
                                                        string column,
                                                        int startRow,
                                                        string[] defaultValues)
        {
            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            if (workSheet == null)
            {
                workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
            }
            else
            {
                workSheet.Cells[string.Format("{0}:{1}", startRow, workSheet.Dimension.End.Row)].Clear();
            }

            for (int emptyRow = startRow, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
            }

        }

    }
}
