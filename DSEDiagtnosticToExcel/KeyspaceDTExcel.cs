using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;

namespace DSEDiagtnosticToExcel
{
    public sealed class KeyspaceExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.Keyspaces;

        public KeyspaceExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public KeyspaceExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
        {
        }

        public override Tuple<IFilePath, string, int> Load()
        {
           var nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.WorkSheetName, this.DataTable,
                                                           (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount, loadRange) =>
                                                            {
                                                                switch (stage)
                                                                {
                                                                    case WorkBookProcessingStage.PreProcess:
                                                                    case WorkBookProcessingStage.PrepareFileName:
                                                                    case WorkBookProcessingStage.PreProcessDataTable:
                                                                        break;
                                                                    case WorkBookProcessingStage.PreLoad:
                                                                        this.CallActionEvent("Begin Loading");
                                                                        break;
                                                                    case WorkBookProcessingStage.PreSave:
                                                                        {
                                                                            var workSheet = excelPackage.Workbook.Worksheets[WorkSheetName];
                                                                            var rangeAddress = loadRange == null ? null : workSheet?.Cells[loadRange];

                                                                            if (rangeAddress != null)
                                                                            {
                                                                                var startRow = rangeAddress.Start.Row;
                                                                                var endRow = rangeAddress.End.Row;
                                                                                string lastValue = string.Empty;
                                                                                string currentValue;
                                                                                bool formatOn = false;

                                                                                for (int nRow = startRow; nRow <= endRow; ++nRow)
                                                                                {
                                                                                    if (workSheet.Cells[nRow, 1] != null)
                                                                                    {
                                                                                        currentValue = workSheet.Cells[nRow, 1].Value as string;
                                                                                        if (currentValue != null)
                                                                                        {
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

                                                                                            if (formatOn)
                                                                                            {
                                                                                                workSheet.Row(nRow).Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                                                                                                workSheet.Row(nRow).Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                                            }
                                                                                            else
                                                                                            {
                                                                                                workSheet.Row(nRow).Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.None;
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }

                                                                            this.CallActionEvent("Loaded");
                                                                        }
                                                                        break;
                                                                    case WorkBookProcessingStage.Saved:
                                                                        this.CallActionEvent("Workbook Saved");
                                                                        break;
                                                                    case WorkBookProcessingStage.PostProcess:
                                                                        break;
                                                                    default:
                                                                        break;
                                                                }
                                                            },
                                                            workSheet =>
                                                            {
                                                                var dtKeySpace = this.DataTable;

                                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                workSheet.Cells["D:D"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["E:E"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["M:M"].Style.Numberformat.Format = "#,##0";
                                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "#,##0";
                                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["P:P"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["Q:Q"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["R:R"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["S:S"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["T:T"].Style.Numberformat.Format = "#,###";
                                                                workSheet.Cells["U:U"].Style.Numberformat.Format = "#,###";

                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 5].FormulaR1C1 = string.Format("sum(E2:E{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 6].FormulaR1C1 = string.Format("sum(F2:F{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 7].FormulaR1C1 = string.Format("sum(G2:G{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 8].FormulaR1C1 = string.Format("sum(H2:H{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 9].FormulaR1C1 = string.Format("sum(I2:I{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 10].FormulaR1C1 = string.Format("sum(J2:J{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 11].FormulaR1C1 = string.Format("sum(K2:K{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 12].FormulaR1C1 = string.Format("sum(L2:L{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 13].FormulaR1C1 = string.Format("sum(M2:M{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 14].FormulaR1C1 = string.Format("sum(N2:N{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 15].FormulaR1C1 = string.Format("sum(O2:O{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 16].FormulaR1C1 = string.Format("sum(P2:P{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 17].FormulaR1C1 = string.Format("sum(Q2:Q{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 18].FormulaR1C1 = string.Format("sum(R2:R{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 19].FormulaR1C1 = string.Format("sum(S2:S{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 20].FormulaR1C1 = string.Format("sum(T2:T{0})", dtKeySpace.Rows.Count + 1);
                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 21].FormulaR1C1 = string.Format("sum(U2:U{0})", dtKeySpace.Rows.Count + 1);

                                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 5, dtKeySpace.Rows.Count + 2, 21].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Thick;

                                                                workSheet.View.FreezePanes(2, 2);
                                                                workSheet.Cells["A1:U1"].AutoFilter = true;
                                                                
                                                                workSheet.AutoFitColumn(workSheet.Cells["A:U"]);
                                                            },
                                                            -1,
                                                           -1,
                                                           "A1",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet,
                                                           cachePackage: LibrarySettings.ExcelPackageCache,
                                                           saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                           excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
