using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;
using DT = DSEDiagnosticToDataTable;

namespace DSEDiagtnosticToExcel
{
    public sealed class NodeStateExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.NodeState;

        public NodeStateExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(loginfoDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public NodeStateExcel(DataTable loginfoDataTable,
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
                                                                        this.CallActionEvent("Loaded");
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

                                                                 this.DataTable.GetColumn(DT.ColumnNames.UTCTimeStamp)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);
                                                                 this.DataTable.GetColumn(DT.ColumnNames.LogLocalTimeStamp)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);

                                                                 //this.DataTable.GetColumn(DT.ColumnNames.DataCenter);
                                                                 //this.DataTable.GetColumn(DT.ColumnNames.NodeIPAddress);                                                                                                                                 
                                                                 //this.DataTable.GetColumn(DT.NodeStateDataTable.Columns.State);

                                                                 this.DataTable.GetColumn(DT.NodeStateDataTable.Columns.SortOrder)
                                                                    .HideColumn();
                                                                 this.DataTable.GetColumn(DT.NodeStateDataTable.Columns.Duration)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanSecsFormat);
                                                                 
                                                                 workSheet.UpdateWorksheet(this.DataTable, 1);

                                                                 workSheet.AutoFitColumn(this.DataTable);

                                                                 var wsName = "NodeStateTable";
                                                                 if (splitNbr.HasValue && splitNbr.Value > 1)
                                                                 {
                                                                     wsName += splitNbr.Value.ToString("000");
                                                                 }

                                                                 workSheet.CreateExcelTable(this.DataTable, wsName);                                                                 
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
