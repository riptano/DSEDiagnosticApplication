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
    public sealed class LogInformationExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.LogInfo;

        public LogInformationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(loginfoDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public LogInformationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook = null)
            : this(loginfoDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
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
                                                             (workSheet, splitNbr) =>
                                                             {
                                                                 workSheet.Cells["M:M"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Right;
                                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                workSheet.View.FreezePanes(2, 1);

                                                                workSheet.Cells["E:E"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeFormat;
                                                                workSheet.Cells["G:G"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["H:H"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["I:I"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["I:I"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                workSheet.Cells["J:J"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["K:K"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["L:L"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["L:L"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["P:P"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["R:R"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["S:S"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;

                                                                //Column Group B, C, D, E
                                                                workSheet.Column(2).OutlineLevel = 1;
                                                                workSheet.Column(2).Collapsed = true;
                                                                workSheet.Column(3).OutlineLevel = 1;
                                                                workSheet.Column(3).Collapsed = true;
                                                                workSheet.Column(4).OutlineLevel = 1;
                                                                workSheet.Column(4).Collapsed = true;
                                                                workSheet.Column(5).OutlineLevel = 1;
                                                                workSheet.Column(5).Collapsed = true;

                                                                 //Column Group K, L, M
                                                                 workSheet.Column(10).OutlineLevel = 1;
                                                                workSheet.Column(10).Collapsed = true;
                                                                workSheet.Column(11).OutlineLevel = 1;
                                                                workSheet.Column(11).Collapsed = true;
                                                                workSheet.Column(12).OutlineLevel = 1;
                                                                workSheet.Column(12).Collapsed = true;

                                                                workSheet.Cells["A1:T1"].AutoFilter = true;
                                                                
                                                                workSheet.AutoFitColumn(workSheet.Cells["A:L"], workSheet.Cells["N:T"]);
                                                                workSheet.Column(13).Width = 27; //M
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
