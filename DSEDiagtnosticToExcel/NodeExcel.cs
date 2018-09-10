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
    public sealed class NodeExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.Node;

        public NodeExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public NodeExcel(DataTable keyspaceDataTable,
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
                                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                workSheet.View.FreezePanes(3, 2);

                                                                workSheet.Cells["G1:L1"].Style.WrapText = true;
                                                                workSheet.Cells["G1:L1"].Merge = true;
                                                                workSheet.Cells["G1:L1"].Value = "Versions";
                                                                workSheet.Cells["G1:G2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                                workSheet.Cells["L1:L2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;


                                                                workSheet.Cells["A2:AN2"].AutoFilter = true;                                                               
                                                                workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "##0.00%";
                                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "0.00";
                                                                
                                                                workSheet.Cells["Q:Q"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["R:R"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["S:S"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat; // "0.00";
                                                               
                                                                workSheet.Cells["T:T"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["U:U"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["V:V"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["W:W"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["X:X"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["Y:Y"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["Z:Z"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                workSheet.Cells["AA:AA"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["AB:AB"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                workSheet.Cells["AC:AC"].Style.Numberformat.Format = "#,###,###,##0";

                                                                workSheet.Cells["AE:AE"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["AF:AF"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AG:AG"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["AH:AH"].Style.Numberformat.Format = "#,###,###,##0.0000%";

                                                                //workSheet.InsertColumn(11, 1);
                                                                //workSheet.Column(10).Hidden = true;
                                                                //workSheet.Cells["J1"].Value = "Uptime (Days)";
                                                                //workSheet.Cells["K1"].Value = "Uptime";
                                                                //workSheet.Cells[string.Format("K2:K{0}", dtRingInfo.Rows.Count + 2)].FormulaR1C1 = "CONCATENATE(TEXT(FLOOR(J2,1),\"@\"),\" \",TEXT(J2,\"hh:mm:ss\"))";

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
