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
    public sealed class NodeConfigChangesExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.NodeConfigChanges;

        public NodeConfigChangesExcel(DataTable keyspaceDataTable,
                                        IFilePath excelTargetWorkbook,
                                        IFilePath excelTemplateWorkbook,
                                        string worksheetName,
                                        bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public NodeConfigChangesExcel(DataTable keyspaceDataTable,
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

                                                                 this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.LogLocalTimeStamp)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);
                                                                 this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.UTCTimeStamp)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);
                                                                 
                                                                 workSheet.UpdateWorksheet(this.DataTable, 1);

                                                                 workSheet.AutoFitColumn(this.DataTable);

                                                                 workSheet.TranslaateToColumnRange(this.DataTable,
                                                                                                    DSEDiagnosticToDataTable.ColumnNames.DataCenter,
                                                                                                    "Current Value",
                                                                                                    1, 1).AutoFilter = true;                                                                
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
