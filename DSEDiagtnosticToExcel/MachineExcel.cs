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
    public sealed class MachineExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.Machine;

        public MachineExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public MachineExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
        { }

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
                                                                                    if (workSheet.Cells[nRow, 2] != null)
                                                                                    {
                                                                                        currentValue = workSheet.Cells[nRow, 2].Value as string;
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
                                                                workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                workSheet.View.FreezePanes(3, 2);

                                                                workSheet.Cells["K1:N1"].Style.WrapText = true;
                                                                workSheet.Cells["K1:N1"].Merge = true;
                                                                workSheet.Cells["K1:N1"].Value = "CPU Load (Percent)";
                                                                workSheet.Cells["K1:K2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                workSheet.Cells["N1:N2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                                workSheet.Cells["O1:T1"].Style.WrapText = true;
                                                                workSheet.Cells["O1:T1"].Merge = true;
                                                                workSheet.Cells["O1:T1"].Value = "Memory (MB)";
                                                                workSheet.Cells["O1:O2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                workSheet.Cells["T1:T2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                                workSheet.Cells["U1:Y1"].Style.WrapText = true;
                                                                workSheet.Cells["U1:Y1"].Merge = true;
                                                                workSheet.Cells["U1:Y1"].Value = "Java";
                                                                workSheet.Cells["U1:U2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                workSheet.Cells["Y1:Y2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;

                                                                workSheet.Cells["Z1:AC1"].Style.WrapText = true;
                                                                workSheet.Cells["Z1:AC1"].Merge = true;
                                                                workSheet.Cells["Z1:AC1"].Value = "Java Non-Heap (MB)";
                                                                workSheet.Cells["Z1:Z2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;
                                                                workSheet.Cells["AC1:AC2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;

                                                                workSheet.Cells["AD1:AG1"].Style.WrapText = true;
                                                                workSheet.Cells["AD1:AG1"].Merge = true;
                                                                workSheet.Cells["AD1:AG1"].Value = "Java Heap (MB)";
                                                                workSheet.Cells["AD1:AD2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;
                                                                workSheet.Cells["AG1:AG2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                
                                                                workSheet.Cells["AH1:AO1"].Style.WrapText = true;
                                                                workSheet.Cells["AH1:AO1"].Merge = true;
                                                                workSheet.Cells["AH1:AO1"].Value = "NTP";
                                                                workSheet.Cells["AH1:AO2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                workSheet.Cells["AH1:AO2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                                workSheet.Cells["E:E"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["P:P"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["Q:Q"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["R:R"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["S:S"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["T:T"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["V:V"].Style.Numberformat.Format = "#,###,###,##0";

                                                                workSheet.Cells["Z:Z"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["AA:AA"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["AB:AB"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["AC:AC"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                                workSheet.Cells["AD:AD"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["AE:AE"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["AF:AF"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["AG:AG"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                                workSheet.Cells["AH:AH"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AI:AI"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AJ:AJ"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AK:AK"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AL:AL"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AM:AM"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AN:AN"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AO:AO"].Style.Numberformat.Format = "#,###,###,##0";

                                                                workSheet.Cells["A2:AO2"].AutoFilter = true;
                                                                
                                                                workSheet.AutoFitColumn();
                                                            },
                                                            -1,
                                                           -1,
                                                           "A2",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet,
                                                           cachePackage: LibrarySettings.ExcelPackageCache,
                                                           saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                           excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
