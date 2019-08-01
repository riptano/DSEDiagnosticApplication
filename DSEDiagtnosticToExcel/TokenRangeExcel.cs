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
    public sealed class TokenRangexcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.TokenRanges;

        public TokenRangexcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public TokenRangexcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook = null)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
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

                                                                            workSheet.AltFileFillRow(2,
                                                                                                       this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress));
                                                                            
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
                                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                workSheet.View.FreezePanes(2, 1);
                                                                workSheet.Cells["A1:F1"].AutoFilter = true;
                                                                //workSheet.Cells["C:D"].Style.Numberformat.Format = "TEXT";
                                                                //workSheet.Cells["E:E"].Style.Numberformat.Format = "TEXT";
                                                                workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                
                                                                workSheet.AutoFitColumn();
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
