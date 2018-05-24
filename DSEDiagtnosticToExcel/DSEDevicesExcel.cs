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
    public sealed class DSEDevicesExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.DSEDevices;

        public DSEDevicesExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public DSEDevicesExcel(DataTable keyspaceDataTable,
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
                                                                 workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(3, 1);

                                                                 workSheet.Cells["C1:J1"].Style.WrapText = true;
                                                                 workSheet.Cells["C1:J1"].Merge = true;
                                                                 workSheet.Cells["C1:J1"].Value = "Devices";
                                                                 workSheet.Cells["C1:C2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                 workSheet.Cells["J1:J2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                                 workSheet.Cells["D:D"].Style.Numberformat.Format = "0%";
                                                                 workSheet.Cells["F:F"].Style.Numberformat.Format = "0%";
                                                                 workSheet.Cells["H:H"].Style.Numberformat.Format = "0%";
                                                                 workSheet.Cells["J:J"].Style.Numberformat.Format = "0%";

                                                                 workSheet.Cells["A2:J2"].AutoFilter = true;

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
