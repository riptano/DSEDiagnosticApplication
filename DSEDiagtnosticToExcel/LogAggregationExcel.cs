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
    public sealed class LogAggregationExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.LogAggregation;

        public LogAggregationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(loginfoDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public LogAggregationExcel(DataTable loginfoDataTable,
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
                                                             workSheet =>
                                                             {
                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(2, 1);
                                                                 
                                                                 workSheet.Cells["A:A"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                 workSheet.Cells["B:B"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                 workSheet.Cells["C:C"].Style.Numberformat.Format = Properties.Settings.Default.ExcelTimeSpanFormat;
                                                                 workSheet.Cells["M:M"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                 workSheet.Cells["N:N"].Style.Numberformat.Format = Properties.Settings.Default.ExcelDateTimeFormat;
                                                                 workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###,###,##0";

                                                                 workSheet.Cells["Q:Q"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["R:R"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["S:S"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["T:T"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["U:U"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["V:V"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["W:W"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["X:X"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                                 workSheet.Cells["Y:Y"].Style.Numberformat.Format = "#,###,###,##0.000";

                                                                 workSheet.Cells["Z:Z"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["AA:AA"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["AB:AB"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["AC:AC"].Style.Numberformat.Format = "#,###,###,##0";

                                                                 workSheet.Cells["AD:AD"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AE:AE"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AF:AF"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AG:AG"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AH:AH"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AI:AI"].Style.Numberformat.Format = "#,###,###,##0.00####";

                                                                 workSheet.Cells["AJ:AJ"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AK:AK"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AL:AL"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AM:AM"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AN:AN"].Style.Numberformat.Format = "#,###,###,##0.00####";

                                                                 workSheet.Cells["AO:AO"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["AP:AP"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["AQ:AQ"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                 workSheet.Cells["AR:AR"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                 workSheet.Cells["AS:AS"].Style.Numberformat.Format = "#,###,###,##0";

                                                                 workSheet.Cells["AT:AT"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["AU:AU"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["AV:AV"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                 workSheet.Cells["AW:AW"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                                 workSheet.Cells["AX:AX"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AY:AY"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["AZ:AZ"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["BA:BA"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["BB:BB"].Style.Numberformat.Format = "#,###,###,##0.00####";

                                                                 workSheet.Cells["BC:BC"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["BD:BD"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["BE:BE"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["BF:BF"].Style.Numberformat.Format = "#,###,###,##0.00####";
                                                                 workSheet.Cells["BG:BG"].Style.Numberformat.Format = "#,###,###,##0.00####";

                                                                 //workSheet.Cells["A1:AR1"].AutoFilter = true;

                                                                 workSheet.AutoFitColumn(workSheet.Cells["A:H"], workSheet.Cells["J:J"], workSheet.Cells["M:BG"]);
                                                                 workSheet.Column(7).Width = 8; //h
                                                                 workSheet.Column(9).Width = 9; //I
                                                                 workSheet.Column(11).Width = 11; //K
                                                                 workSheet.Column(12).Width = 12; //L

                                                                 var table = workSheet.Tables.FirstOrDefault(t => t.Name == "AggregatedLogTable");
                                                                 if (table == null)
                                                                 {
                                                                     using (var tblRange = workSheet.Cells[string.Format("A1:BG{0}", this.DataTable.Rows.Count + 2)])
                                                                     {
                                                                         table = workSheet.Tables.Add(tblRange, "AggregatedLogTable");

                                                                         table.ShowFilter = true;
                                                                         table.ShowHeader = true;
                                                                         table.TableStyle = OfficeOpenXml.Table.TableStyles.Light21;
                                                                     }
                                                                 }
                                                                 else
                                                                 {
                                                                     var oldaddy = table.Address;
                                                                     var newaddy = new ExcelAddressBase(oldaddy.Start.Row, oldaddy.Start.Column, this.DataTable.Rows.Count + 2, oldaddy.End.Column);

                                                                     //Edit the raw XML by searching for all references to the old address
                                                                     table.TableXml.InnerXml = table.TableXml.InnerXml.Replace(oldaddy.ToString(), newaddy.ToString());
                                                                 }
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

