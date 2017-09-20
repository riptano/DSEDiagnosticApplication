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
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(loginfoDataTable, excelTargetWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public LogInformationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook)
            : this(loginfoDataTable, excelTargetWorkbook, null, true)
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
                                                                 workSheet.Cells["M:M"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Right;
                                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                workSheet.View.FreezePanes(2, 1);

                                                                workSheet.Cells["E:E"].Style.Numberformat.Format = "hh:mm";
                                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                workSheet.Cells["H:H"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "d hh:mm";
                                                                workSheet.Cells["I:I"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "d hh:mm";
                                                                workSheet.Cells["L:L"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "#,###,###,##0";
                                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                workSheet.Cells["P:P"].Style.Numberformat.Format = "d hh:mm";
                                                                workSheet.Cells["R:R"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                workSheet.Cells["S:S"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";

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

                                                                workSheet.Cells["A1:S1"].AutoFilter = true;
                                                                
                                                                workSheet.AutoFitColumn(workSheet.Cells["A:L"], workSheet.Cells["N:S"]);
                                                                workSheet.Column(13).Width = 27; //M
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
