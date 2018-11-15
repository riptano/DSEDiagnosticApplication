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

                                                                            workSheet.AltFileFillRow(3,
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
                                                             workSheet =>
                                                             {
                                                                 workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(3, 1);

                                                                 //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
                                                                 //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter);
                                                                 
                                                                 this.DataTable.SetGroupHeader("Devices", -2, true,
                                                                        this.DataTable.GetColumn("Data"),
                                                                        this.DataTable.GetColumn("Data Utilization")
                                                                            .SetNumericFormat("0%")
                                                                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorageUtilization),
                                                                        this.DataTable.GetColumn("Commit Log"),
                                                                        this.DataTable.GetColumn("Commit Utilization")
                                                                            .SetNumericFormat("0%")
                                                                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorageUtilization),
                                                                        this.DataTable.GetColumn("Saved Cache"),
                                                                        this.DataTable.GetColumn("Cache Utilization")
                                                                            .SetNumericFormat("0%")
                                                                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonStorageUtilization),
                                                                        this.DataTable.GetColumn("Other"),
                                                                        this.DataTable.GetColumn("Other Utilization")
                                                                            .SetNumericFormat("0%")
                                                                    );
                                                                                                                                 
                                                                 workSheet.UpdateWorksheet(this.DataTable, 2);

                                                                 workSheet.ExcelRange(2,
                                                                                       this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress),
                                                                                       this.DataTable.GetColumn("Other Utilization"))
                                                                             .First().AutoFilter = true;

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
