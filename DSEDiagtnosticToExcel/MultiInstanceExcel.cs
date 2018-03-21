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
    public class MultiInstanceExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.MultiInstanceInfo;

        public MultiInstanceExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public MultiInstanceExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook)
            : this(keyspaceDataTable, excelTargetWorkbook, null, true)
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
                                                                                                workSheet.Row(nRow).Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                                                //workSheet.Row(nRow).Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.Yellow);                                                                                                
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
                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(2, 1);
                                                                 workSheet.Cells["A1:F1"].AutoFilter = true;
                                                                 
                                                                 workSheet.AutoFitColumn();
                                                             },
                                                             -1,
                                                            -1,
                                                            "A1",
                                                            this.UseDataTableDefaultView,
                                                            appendToWorkSheet: this.AppendToWorkSheet);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }

    }
}
