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
        public enum WorkBookProcessingStage
        {
            PreProcess,
            PreProcessDataTable,
            PrepareFileName,
            PreLoad,
            PreSave,
            Saved,
            PostProcess
        }

        static public int DifferentExcelWorkBook(string excelFilePath,
                                                    string workSheetName,
                                                    DataTable dtExcel,
                                                    Action<WorkBookProcessingStage, IFilePath, IFilePath, string, ExcelPackage, DataTable, int> workBookActions,
                                                    Action<ExcelWorksheet> worksheetAction = null,
                                                    int maxRowInExcelWorkBook = -1,
                                                    int maxRowInExcelWorkSheet = -1,
                                                    Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                    string startingWSCell = "A1",
                                                    bool bypassPreprocessAction = false)
        {
            var excelTargetFile = Common.Path.PathUtils.BuildFilePath(excelFilePath);
            var orgTargetFile = (IFilePath)excelTargetFile.Clone();

            workBookActions?.Invoke(bypassPreprocessAction
                                            ? WorkBookProcessingStage.PreProcessDataTable
                                            : WorkBookProcessingStage.PreProcess,
                                        orgTargetFile,
                                        null,
                                        workSheetName,
                                        null,
                                        dtExcel,
                                        -1);

            if (dtExcel.Rows.Count == 0)
            {
                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, null, null, dtExcel, 0);
                return 0;
            }

            if (maxRowInExcelWorkBook <= 0 || dtExcel.Rows.Count <= maxRowInExcelWorkSheet)
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}{1}",
                                                                        excelTargetFile.Name,
                                                                        string.IsNullOrEmpty(ParserSettings.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : ParserSettings.ExcelWorkBookFileExtension);

                var excelFile = ((IFilePath)excelTargetFile.Clone()).ApplyFileNameFormat(new object[] { workSheetName });

                workBookActions?.Invoke(WorkBookProcessingStage.PrepareFileName, orgTargetFile, excelFile, workSheetName, null, dtExcel, -1);

                using (var excelPkg = new ExcelPackage(excelFile.FileInfo()))
                {
                    workBookActions?.Invoke(WorkBookProcessingStage.PreLoad, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, -1);

                    WorkSheet(excelPkg,
                                workSheetName,
                                dtExcel,
                                worksheetAction,
                                null,
                                startingWSCell);

                    workBookActions?.Invoke(WorkBookProcessingStage.PreSave, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, dtExcel.Rows.Count);
                    DTLoadIntoExcel.UpdateApplicationWs(excelPkg);
                    excelPkg.Save();
                    workBookActions?.Invoke(WorkBookProcessingStage.Saved, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, dtExcel.Rows.Count);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
                }

                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, workSheetName, null, dtExcel, dtExcel.Rows.Count);
                return dtExcel.Rows.Count;
            }

            var dtSplits = dtExcel.SplitTable(maxRowInExcelWorkBook);
            int nResult = 0;
            long totalRows = 0;

            if (dtSplits.Count() == 1)
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}{1}",
                                                                        excelTargetFile.Name,
                                                                        string.IsNullOrEmpty(ParserSettings.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : ParserSettings.ExcelWorkBookFileExtension);
            }
            else
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}-{{1:000}}{1}",
                                                                excelTargetFile.Name,
                                                                string.IsNullOrEmpty(ParserSettings.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : ParserSettings.ExcelWorkBookFileExtension);
            }

            Parallel.ForEach(dtSplits, dtSplit =>
            //foreach (var dtSplit in dtSplits)
            {
                var excelFile = ((IFilePath)excelTargetFile.Clone())
                                                .ApplyFileNameFormat(new object[] { workSheetName, System.Threading.Interlocked.Increment(ref totalRows) });

                workBookActions?.Invoke(WorkBookProcessingStage.PrepareFileName, orgTargetFile, excelFile, workSheetName, null, dtSplit, -1);

                using (var excelPkg = new ExcelPackage(excelFile.FileInfo()))
                {
                    var newStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();

                    workBookActions?.Invoke(WorkBookProcessingStage.PreLoad, orgTargetFile, excelFile, workSheetName, excelPkg, dtSplit, -1);

                    newStack.Push(dtSplit);

                    WorkSheet(excelPkg,
                                workSheetName,
                                newStack,
                                worksheetAction,
                                maxRowInExcelWorkSheet > 0,
                                maxRowInExcelWorkSheet,
                                null,
                                startingWSCell);

                    System.Threading.Interlocked.Add(ref nResult, dtSplit.Rows.Count);

                    workBookActions?.Invoke(WorkBookProcessingStage.PreSave, orgTargetFile, excelFile, workSheetName, excelPkg, dtSplit, dtSplit.Rows.Count);
                    DTLoadIntoExcel.UpdateApplicationWs(excelPkg);
                    excelPkg.Save();
                    workBookActions?.Invoke(WorkBookProcessingStage.Saved, orgTargetFile, excelFile, workSheetName, excelPkg, dtSplit, dtSplit.Rows.Count);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
                }
            });

            workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, workSheetName, null, null, nResult);

            return nResult;
        }

        static public int DifferentExcelWorkBook(string excelFilePath,
                                                    string workSheetName,
                                                    Common.Patterns.Collections.LockFree.Stack<DataTable> dtExcelStack,
                                                    Action<WorkBookProcessingStage, IFilePath, IFilePath, string, ExcelPackage, DataTable, int> workBookActions,
                                                    Action<ExcelWorksheet> worksheetAction = null,
                                                    int maxRowInExcelWorkBook = -1,
                                                    int maxRowInExcelWorkSheet = -1,
                                                    Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                    string startingWSCell = "A1")
        {
            workBookActions?.Invoke(WorkBookProcessingStage.PreProcess, Common.Path.PathUtils.BuildFilePath(excelFilePath), null, workSheetName, null, null, -1);

            var dtComplete = dtExcelStack.MergeIntoOneDataTable(viewFilterSortRowStateOpts);

            return DifferentExcelWorkBook(excelFilePath,
                                            workSheetName,
                                            dtComplete,
                                            workBookActions,
                                            worksheetAction,
                                            maxRowInExcelWorkBook,
                                            maxRowInExcelWorkSheet,
                                            null,
                                            startingWSCell,
                                            true);
        }

        static public void UpdateApplicationWs(ExcelPackage excelPkg)
        {
            var workSheet = excelPkg.Workbook.Worksheets["Application"];
            if (workSheet == null)
            {
                workSheet = excelPkg.Workbook.Worksheets.Add("Application");
            }

            workSheet.Column(1).Width = 150;
            workSheet.Column(1).Style.WrapText = true;

            workSheet.Cells["A2"].Value = string.Format("Start Timestamp: {0} Duration: {1:hh\\:mm\\:ss\\.ffff}",
                                                            Program.RunDateTime,
                                                            DateTime.Now - Program.RunDateTime);
            workSheet.Cells["A3"].Value = string.Format("Program: {0} Version: {1} Directory: {2}",
                                                            Common.Functions.Instance.ApplicationName,
                                                            Common.Functions.Instance.ApplicationVersion,
                                                            Common.Functions.Instance.AssemblyDir);
            workSheet.Cells["A4"].Value = string.Format("Working Directory: {0}", System.Environment.CurrentDirectory);
            workSheet.Cells["A5"].Value = Program.CommandLineArgsString;
            //workSheet.Cells["A6"].Value = Program.CommandArgsString.Replace("--", "\r\n\t--");

            workSheet.Cells["A7"].Value = string.Format("Warnings: {0} Errors: {1}",
                                                            Program.ConsoleWarnings.Counter > 0,
                                                            Program.ConsoleErrors.Counter > 0);

            /*if (ProcessFileTasks.LogCassandraMaxMinTimestamp.IsEmpty())
            {
                workSheet.Cells["A8"].Value = "No Log Range";
            }
            else
            {
                workSheet.Cells["A8"].Value = string.Format("Log From {0} ({0:ddd}) to {1} ({1:ddd}) duration {2:d\\ hh\\:mm} Number of Log items read/processed: {3:###,###,##0}",
                                                            ProcessFileTasks.LogCassandraMaxMinTimestamp.Min,
                                                            ProcessFileTasks.LogCassandraMaxMinTimestamp.Max,
                                                            ProcessFileTasks.LogCassandraMaxMinTimestamp.Max - ProcessFileTasks.LogCassandraMaxMinTimestamp.Min,
                                                            Program.ConsoleLogCount.Counter);
            }*/
        }
    }
}
