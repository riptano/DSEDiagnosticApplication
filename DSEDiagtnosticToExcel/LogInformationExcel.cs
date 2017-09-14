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
                                                            (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount) =>
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
                                                                 workSheet.Cells["L:L"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Right;
                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(2, 1);

                                                                 workSheet.Cells["F:F"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss zz";
                                                                 workSheet.Cells["G:G"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss zz";
                                                                 workSheet.Cells["H:H"].Style.Numberformat.Format = "d hh:mm";
                                                                 workSheet.Cells["H:H"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 workSheet.Cells["I:I"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss zz";
                                                                 workSheet.Cells["J:J"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss zz";
                                                                 workSheet.Cells["K:K"].Style.Numberformat.Format = "d hh:mm";
                                                                 workSheet.Cells["K:K"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["N:N"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                                 workSheet.Cells["O:O"].Style.Numberformat.Format = "d hh:mm";
                                                                 workSheet.Cells["Q:Q"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                 workSheet.Cells["R:R"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                 workSheet.Cells["S:S"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                 workSheet.Cells["T:T"].Style.Numberformat.Format = "yyyy-mm-dd hh:mm:ss";
                                                                 workSheet.Cells["U:U"].Style.Numberformat.Format = "d hh:mm";

                                                                 //Column Group B, C, D
                                                                 workSheet.Column(2).OutlineLevel = 1;
                                                                 workSheet.Column(2).Collapsed = true;
                                                                 workSheet.Column(3).OutlineLevel = 1;
                                                                 workSheet.Column(3).Collapsed = true;
                                                                 workSheet.Column(4).OutlineLevel = 1;
                                                                 workSheet.Column(4).Collapsed = true;

                                                                 //Column Group J, K, L
                                                                 workSheet.Column(9).OutlineLevel = 1;
                                                                 workSheet.Column(9).Collapsed = true;
                                                                 workSheet.Column(10).OutlineLevel = 1;
                                                                 workSheet.Column(10).Collapsed = true;
                                                                 workSheet.Column(11).OutlineLevel = 1;
                                                                 workSheet.Column(11).Collapsed = true;

                                                                 workSheet.Cells["A1:U1"].AutoFilter = true;
                                                                 workSheet.AutoFitColumn(workSheet.Cells["A:K"], workSheet.Cells["M:U"]);
                                                                 workSheet.Column(12).Width = 27; //L
                                                             },
                                                             -1,
                                                            -1,
                                                            "A1",
                                                            this.UseDataTableDefaultView);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
